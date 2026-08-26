import { mutation } from './_generated/server';
import { v } from 'convex/values';

// One location event, applied the way aa-app's `insertNewLocationPayload` does:
// two run-length-encoding neighbour reads, then either extend the previous
// cluster in place or start a new row, then upsert the device's latest-state
// row. Three indexed reads and one or two writes per event.
const eventValidator = v.object({
  deviceId: v.string(),
  timestamp: v.number(),
  lat: v.number(),
  lng: v.number(),
  speed: v.number(),
  engineOn: v.boolean(),
  // Decided by the caller so the merge ratio is a property of the load, not of
  // floating-point drift maths that would differ between runs.
  merge: v.boolean(),
});

export const ingestBatch = mutation({
  args: { events: v.array(eventValidator) },
  returns: v.object({ inserted: v.number(), merged: v.number() }),
  handler: async (ctx, { events }) => {
    let inserted = 0;
    let merged = 0;

    // Sequential, like aa-app's `for (const … of ordered) await …` — events for
    // one device are order-dependent, and this is the shape being measured.
    for (const e of events) {
      const previous = await ctx.db
        .query('deviceLocations')
        .withIndex('by_device_timestamp', (q) =>
          q.eq('deviceId', e.deviceId).lte('timestamp', e.timestamp)
        )
        .order('desc')
        .first();

      const following = await ctx.db
        .query('deviceLocations')
        .withIndex('by_device_timestamp', (q) =>
          q.eq('deviceId', e.deviceId).gte('timestamp', e.timestamp)
        )
        .order('asc')
        .first();
      void following;

      if (e.merge && previous !== null) {
        await ctx.db.replace(previous._id, {
          ...previous,
          stillDuration: (previous.stillDuration ?? 0) + 1000,
          mergedCount: (previous.mergedCount ?? 1) + 1,
        });
        merged += 1;
      } else {
        await ctx.db.insert('deviceLocations', {
          deviceId: e.deviceId,
          timestamp: e.timestamp,
          lat: e.lat,
          lng: e.lng,
          speed: e.speed,
          engineOn: e.engineOn,
          mergedCount: 1,
        });
        inserted += 1;
      }

      const latest = await ctx.db
        .query('deviceLatestLocations')
        .withIndex('by_device', (q) => q.eq('deviceId', e.deviceId))
        .first();
      const row = {
        deviceId: e.deviceId,
        timestamp: e.timestamp,
        lat: e.lat,
        lng: e.lng,
        geohash: `${Math.floor(e.lat)}:${Math.floor(e.lng)}`,
      };
      if (latest === null) {
        await ctx.db.insert('deviceLatestLocations', row);
      } else {
        await ctx.db.replace(latest._id, row);
      }
    }

    return { inserted, merged };
  },
});
