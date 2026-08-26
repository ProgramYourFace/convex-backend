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

// Whole-tenant summary, for the multi-tenant isolation check: how many rows
// does THIS instance hold, and what do its device ids look like. Each hosted
// instance has its own database, so a tenant that can see a neighbour's device
// prefix here is a tenant whose isolation has failed.
export const tenantSummary = query({
  args: {},
  handler: async (ctx) => {
    const locations = await ctx.db.query('deviceLocations').collect();
    const latest = await ctx.db.query('deviceLatestLocations').collect();
    // Strip the trailing index, not everything from the first digit: tenant
    // names carry digits too, so splitting on /\d/ collapses `t-sys01-dev-12`
    // to `t-sys` and makes every tenant look identical.
    const prefixes = [...new Set(latest.map((d) => d.deviceId.replace(/\d+$/, '')))].sort();
    return {
      deviceLocations: locations.length,
      deviceLatestLocations: latest.length,
      devicePrefixes: prefixes,
      sampleDeviceIds: latest.slice(0, 3).map((d) => d.deviceId).sort(),
    };
  },
});
