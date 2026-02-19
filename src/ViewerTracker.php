<?php

declare(strict_types=1);

namespace Fabriq\Streaming;

use Fabriq\Storage\DbManager;

/**
 * Redis-backed concurrent viewer counter.
 *
 * Tracks the number of active viewers per stream per tenant.
 * Uses Redis sorted sets with TTL-based expiry for accuracy.
 *
 * Each viewer heartbeats periodically; stale entries are pruned.
 */
final class ViewerTracker
{
    /** @var int Seconds before a viewer is considered stale */
    private const VIEWER_TTL = 30;

    public function __construct(
        private readonly DbManager $db,
    ) {}

    /**
     * Register or heartbeat a viewer.
     *
     * Call periodically (e.g., every 15 seconds) to keep the viewer active.
     *
     * @param string $tenantId
     * @param string $streamId
     * @param string $viewerId Unique viewer identifier (user_id or session_id)
     */
    public function heartbeat(string $tenantId, string $streamId, string $viewerId): void
    {
        $key = $this->viewerKey($tenantId, $streamId);
        $now = time();

        $redis = $this->db->redis();
        try {
            // Add/update viewer with current timestamp as score
            $redis->zAdd($key, $now, $viewerId);

            // Set TTL on the key to auto-cleanup after stream ends
            $redis->expire($key, self::VIEWER_TTL * 4);
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    /**
     * Remove a viewer (on disconnect).
     */
    public function remove(string $tenantId, string $streamId, string $viewerId): void
    {
        $key = $this->viewerKey($tenantId, $streamId);

        $redis = $this->db->redis();
        try {
            $redis->zRem($key, $viewerId);
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    /**
     * Get the current viewer count for a stream.
     *
     * Prunes stale viewers before counting.
     */
    public function count(string $tenantId, string $streamId): int
    {
        $key = $this->viewerKey($tenantId, $streamId);
        $cutoff = time() - self::VIEWER_TTL;

        $redis = $this->db->redis();
        try {
            // Remove stale viewers
            $redis->zRemRangeByScore($key, '-inf', (string)$cutoff);

            // Count remaining
            return (int)$redis->zCard($key);
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    /**
     * Get list of active viewer IDs for a stream.
     *
     * @return list<string>
     */
    public function getViewers(string $tenantId, string $streamId): array
    {
        $key = $this->viewerKey($tenantId, $streamId);
        $cutoff = time() - self::VIEWER_TTL;

        $redis = $this->db->redis();
        try {
            $redis->zRemRangeByScore($key, '-inf', (string)$cutoff);
            $members = $redis->zRange($key, 0, -1);
            return is_array($members) ? array_values($members) : [];
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    /**
     * Clean up all viewer data for a stream.
     */
    public function clearStream(string $tenantId, string $streamId): void
    {
        $key = $this->viewerKey($tenantId, $streamId);

        $redis = $this->db->redis();
        try {
            $redis->del($key);
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    // ── Internals ───────────────────────────────────────────────────

    private function viewerKey(string $tenantId, string $streamId): string
    {
        return "stream_viewers:{$tenantId}:{$streamId}";
    }
}

