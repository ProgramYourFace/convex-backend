import { query } from './_generated/server';
import { v } from 'convex/values';

// The dashboard read: where is this device right now.
export const latestForDevice = query({
  args: { deviceId: v.string() },
  handler: async (ctx, { deviceId }) =>
    await ctx.db
      .query('deviceLatestLocations')
      .withIndex('by_device', (q) => q.eq('deviceId', deviceId))
      .first(),
});
