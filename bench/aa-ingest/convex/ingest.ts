import { mutation } from './_generated/server';
import { v } from 'convex/values';

// One location event, applied the way aa-app's ingest does: several indexed
// reads, a comparison against what is already stored, and a write that depends
// on the comparison. Read-compare-modify-write, not append.
//
// Per event, before this mutation writes anything:
//   1. the run-length-encoding neighbour *below* the new timestamp
//   2. the neighbour *above* it, to detect out-of-order arrival
//   3. the device's current latest-state row
//   4. the geohash cell, to decide whether the device changed cell
// Then one or two writes. Batched, that is ~4 index seeks and 1-2 writes per
// event, all inside one transaction — which is what makes the ingest path
// read-heavy rather than write-heavy, and why index-seek cost dominates.
const eventValidator = v.object({
  deviceId: v.string(),
  timestamp: v.number(),
  lat: v.number(),
  lng: v.number(),
  speed: v.number(),
  engineOn: v.boolean(),
  merge: v.boolean(),
});

const geohashOf = (lat: number, lng: number) =>
  `${Math.floor(lat * 100)}:${Math.floor(lng * 100)}`;

export const ingestBatch = mutation({
  args: { events: v.array(eventValidator) },
  returns: v.object({ inserted: v.number(), merged: v.number(), moved: v.number() }),
  handler: async (ctx, { events }) => {
    let inserted = 0;
    let merged = 0;
    let moved = 0;

    // Sequential, like aa-app's `for (const … of ordered) await …` — events for
    // one device are order-dependent, and this is the shape being measured.
    for (const e of events) {
      // (1) The run-length-encoded predecessor.
      const previous = await ctx.db
        .query('deviceLocations')
        .withIndex('by_device_timestamp', (q) =>
          q.eq('deviceId', e.deviceId).lte('timestamp', e.timestamp)
        )
        .order('desc')
        .first();

      // (2) The successor, so out-of-order arrival can be detected rather than
      // silently appended in the wrong place.
      const following = await ctx.db
        .query('deviceLocations')
        .withIndex('by_device_timestamp', (q) =>
          q.eq('deviceId', e.deviceId).gte('timestamp', e.timestamp)
        )
        .order('asc')
        .first();
      const outOfOrder = following !== null;

      // (3) The device's current state, which the comparison below needs.
      const latest = await ctx.db
        .query('deviceLatestLocations')
        .withIndex('by_device', (q) => q.eq('deviceId', e.deviceId))
        .first();

      const geohash = geohashOf(e.lat, e.lng);

      // (4) Who else is in this geohash cell — the read that makes this a real
      // compare rather than a blind write.
      const neighbours = await ctx.db
        .query('deviceLatestLocations')
        .withIndex('by_geohash', (q) => q.eq('geohash', geohash))
        .take(4);

      const changedCell = latest !== null && latest.geohash !== geohash;
      if (changedCell) {
        moved += 1;
      }

      // Extend the previous cluster in place, or start a new row. Merging only
      // when the device has not moved cells is the compare-then-modify: the
      // write depends on what the reads found.
      if (e.merge && previous !== null && !outOfOrder && !changedCell) {
        await ctx.db.patch(previous._id, {
          stillDuration: (previous.stillDuration ?? 0) + 1000,
          mergedCount: (previous.mergedCount ?? 1) + 1,
          neighbourCount: neighbours.length,
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
          neighbourCount: neighbours.length,
        });
        inserted += 1;
      }

      const row = {
        deviceId: e.deviceId,
        timestamp: e.timestamp,
        lat: e.lat,
        lng: e.lng,
        geohash,
      };
      if (latest === null) {
        await ctx.db.insert('deviceLatestLocations', row);
      } else {
        await ctx.db.replace(latest._id, row);
      }
    }

    return { inserted, merged, moved };
  },
});
