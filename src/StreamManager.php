<?php

declare(strict_types=1);

namespace Fabriq\Streaming;

use Fabriq\Kernel\Config;
use Fabriq\Observability\Logger;
use Fabriq\Storage\DbManager;

/**
 * Live stream lifecycle manager.
 *
 * Manages stream creation, start/stop, stream key authentication,
 * and metadata. Coordinates between SignalingHandler, TranscodingPipeline,
 * and ViewerTracker.
 *
 * Stream states: pending → live → ended
 */
final class StreamManager
{
    /** @var array<string, array{
     *     stream_id: string,
     *     tenant_id: string,
     *     user_id: string,
     *     stream_key: string,
     *     status: string,
     *     title: string,
     *     started_at: ?int,
     *     ended_at: ?int,
     *     metadata: array<string, mixed>,
     * }> streamId → stream info */
    private array $streams = [];

    private int $streamKeyTtl;

    public function __construct(
        private readonly DbManager $db,
        private readonly ?Config $config = null,
        private readonly ?Logger $logger = null,
    ) {
        $this->streamKeyTtl = (int)($config?->get('streaming.stream_key_ttl', 86400) ?? 86400);
    }

    /**
     * Create a new stream and generate a stream key.
     *
     * @param string $tenantId
     * @param string $userId
     * @param string $title
     * @param array<string, mixed> $metadata
     * @return array{stream_id: string, stream_key: string}
     */
    public function createStream(
        string $tenantId,
        string $userId,
        string $title = '',
        array $metadata = [],
    ): array {
        $streamId = $this->generateStreamId();
        $streamKey = $this->generateStreamKey();

        $this->streams[$streamId] = [
            'stream_id' => $streamId,
            'tenant_id' => $tenantId,
            'user_id' => $userId,
            'stream_key' => $streamKey,
            'status' => 'pending',
            'title' => $title,
            'started_at' => null,
            'ended_at' => null,
            'metadata' => $metadata,
        ];

        // Store stream key in Redis with TTL for validation
        $redis = $this->db->redis();
        try {
            $redis->setEx(
                "stream_key:{$tenantId}:{$streamId}",
                $this->streamKeyTtl,
                $streamKey,
            );
        } finally {
            $this->db->releaseRedis($redis);
        }

        $this->logger?->info('Stream created', [
            'stream_id' => $streamId,
            'tenant_id' => $tenantId,
            'user_id' => $userId,
        ]);

        return [
            'stream_id' => $streamId,
            'stream_key' => $streamKey,
        ];
    }

    /**
     * Validate a stream key for authentication.
     */
    public function validateStreamKey(string $tenantId, string $streamId, string $streamKey): bool
    {
        if ($streamKey === '') {
            return false;
        }

        $redis = $this->db->redis();
        try {
            $storedKey = $redis->get("stream_key:{$tenantId}:{$streamId}");
            return $storedKey === $streamKey;
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    /**
     * Start a stream (transition from pending → live).
     */
    public function startStream(string $streamId): bool
    {
        if (!isset($this->streams[$streamId])) {
            return false;
        }

        $this->streams[$streamId]['status'] = 'live';
        $this->streams[$streamId]['started_at'] = time();

        // Publish stream status to Redis for cross-worker awareness
        $redis = $this->db->redis();
        try {
            $redis->hSet('active_streams', $streamId, json_encode($this->streams[$streamId], JSON_THROW_ON_ERROR));
        } finally {
            $this->db->releaseRedis($redis);
        }

        $this->logger?->info('Stream started', ['stream_id' => $streamId]);

        return true;
    }

    /**
     * End a stream (transition to ended).
     */
    public function endStream(string $streamId): bool
    {
        if (!isset($this->streams[$streamId])) {
            return false;
        }

        $this->streams[$streamId]['status'] = 'ended';
        $this->streams[$streamId]['ended_at'] = time();

        // Remove from active streams in Redis
        $redis = $this->db->redis();
        try {
            $redis->hDel('active_streams', $streamId);
            $redis->del("stream_key:{$this->streams[$streamId]['tenant_id']}:{$streamId}");
        } finally {
            $this->db->releaseRedis($redis);
        }

        $this->logger?->info('Stream ended', ['stream_id' => $streamId]);

        return true;
    }

    /**
     * Get stream info by ID.
     *
     * @return array<string, mixed>|null
     */
    public function getStream(string $streamId): ?array
    {
        return $this->streams[$streamId] ?? null;
    }

    /**
     * Get all live streams for a tenant.
     *
     * @return list<array<string, mixed>>
     */
    public function getLiveStreams(string $tenantId): array
    {
        return array_values(array_filter(
            $this->streams,
            fn(array $s) => $s['tenant_id'] === $tenantId && $s['status'] === 'live',
        ));
    }

    /**
     * Get all active streams across all tenants (from Redis).
     *
     * @return list<array<string, mixed>>
     */
    public function getAllActiveStreams(): array
    {
        $redis = $this->db->redis();
        try {
            $raw = $redis->hGetAll('active_streams');
            if (!is_array($raw)) {
                return [];
            }

            $streams = [];
            foreach ($raw as $data) {
                $decoded = json_decode($data, true);
                if (is_array($decoded)) {
                    $streams[] = $decoded;
                }
            }
            return $streams;
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    /**
     * Get stats for monitoring.
     *
     * @return array{total_streams: int, live_streams: int}
     */
    public function stats(): array
    {
        $live = count(array_filter($this->streams, fn(array $s) => $s['status'] === 'live'));

        return [
            'total_streams' => count($this->streams),
            'live_streams' => $live,
        ];
    }

    // ── Internals ───────────────────────────────────────────────────

    private function generateStreamId(): string
    {
        return 'stream_' . bin2hex(random_bytes(12));
    }

    private function generateStreamKey(): string
    {
        return 'sk_' . bin2hex(random_bytes(24));
    }
}

