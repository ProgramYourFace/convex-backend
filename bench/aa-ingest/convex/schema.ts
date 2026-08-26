import { defineSchema, defineTable } from 'convex/server';
import { v } from 'convex/values';

// Shaped after aa-app's device-location tables: an append-mostly time series
// with a run-length-encoded spine, plus a bounded latest-state row per device.
export default defineSchema({
  deviceLocations: defineTable({
    deviceId: v.string(),
    timestamp: v.number(),
    lat: v.number(),
    lng: v.number(),
    speed: v.number(),
    engineOn: v.boolean(),
    stillDuration: v.optional(v.number()),
    mergedCount: v.optional(v.number()),
    neighbourCount: v.optional(v.number()),
  }).index('by_device_timestamp', ['deviceId', 'timestamp']),

  deviceLatestLocations: defineTable({
    deviceId: v.string(),
    timestamp: v.number(),
    lat: v.number(),
    lng: v.number(),
    geohash: v.string(),
  })
    .index('by_device', ['deviceId'])
    .index('by_geohash', ['geohash']),
});
