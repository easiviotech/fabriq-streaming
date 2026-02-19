<?php

declare(strict_types=1);

namespace Fabriq\Streaming;

use Fabriq\Kernel\Context;
use Fabriq\Observability\Logger;
use Fabriq\Storage\DbManager;
use Swoole\WebSocket\Frame;
use Swoole\WebSocket\Server as WsServer;

/**
 * WebRTC Signaling Handler.
 *
 * Handles SDP offer/answer and ICE candidate exchange over WebSocket.
 * Replaces expensive hosted signaling services (Agora, LiveKit, etc.).
 *
 * Message types handled:
 *   - offer:     Streamer sends SDP offer → relayed to viewers
 *   - answer:    Viewer sends SDP answer → relayed to streamer
 *   - candidate: ICE candidate exchange (both directions)
 *   - subscribe: Viewer subscribes to a stream
 *
 * For small audiences (≤50): P2P media flows directly between peers.
 * For large audiences: TranscodingPipeline + HLS takes over.
 */
final class SignalingHandler
{
    /** @var array<string, array{fd: int, tenant_id: string, user_id: string}> streamId → streamer info */
    private array $streamers = [];

    /** @var array<string, list<int>> streamId → [viewer fd, ...] */
    private array $viewers = [];

    /** @var array<int, string> fd → streamId (for cleanup on disconnect) */
    private array $fdToStream = [];

    public function __construct(
        private readonly StreamManager $streamManager,
        private readonly DbManager $db,
        private readonly ?Logger $logger = null,
    ) {}

    /**
     * Handle an incoming WebSocket signaling message.
     *
     * @param WsServer $server
     * @param Frame $frame
     * @param string $tenantId
     * @param string $userId
     */
    public function handle(WsServer $server, Frame $frame, string $tenantId, string $userId): void
    {
        $data = json_decode($frame->data, true);
        if (!is_array($data)) {
            $server->push($frame->fd, json_encode(['error' => 'Invalid JSON']));
            return;
        }

        $type = $data['type'] ?? '';

        match ($type) {
            'offer' => $this->handleOffer($server, $frame->fd, $tenantId, $userId, $data),
            'answer' => $this->handleAnswer($server, $frame->fd, $data),
            'candidate' => $this->handleCandidate($server, $frame->fd, $data),
            'subscribe' => $this->handleSubscribe($server, $frame->fd, $tenantId, $userId, $data),
            default => $server->push($frame->fd, json_encode([
                'error' => 'Unknown signaling type',
                'type' => $type,
            ])),
        };
    }

    /**
     * Handle streamer disconnect — clean up stream and notify viewers.
     */
    public function handleDisconnect(WsServer $server, int $fd): void
    {
        $streamId = $this->fdToStream[$fd] ?? null;
        if ($streamId === null) {
            return;
        }

        // If the disconnecting fd is the streamer, end the stream
        if (isset($this->streamers[$streamId]) && $this->streamers[$streamId]['fd'] === $fd) {
            $this->notifyViewers($server, $streamId, [
                'type' => 'stream_ended',
                'stream_id' => $streamId,
            ]);

            unset($this->streamers[$streamId]);
            unset($this->viewers[$streamId]);

            $this->logger?->info('Stream ended due to streamer disconnect', [
                'stream_id' => $streamId,
            ]);
        } else {
            // Viewer disconnect — remove from viewer list
            if (isset($this->viewers[$streamId])) {
                $this->viewers[$streamId] = array_values(
                    array_filter($this->viewers[$streamId], fn(int $f) => $f !== $fd)
                );
            }
        }

        unset($this->fdToStream[$fd]);
    }

    /**
     * Get stats for monitoring.
     *
     * @return array{active_streams: int, total_viewers: int}
     */
    public function stats(): array
    {
        $totalViewers = 0;
        foreach ($this->viewers as $fds) {
            $totalViewers += count($fds);
        }

        return [
            'active_streams' => count($this->streamers),
            'total_viewers' => $totalViewers,
        ];
    }

    // ── Internal Handlers ───────────────────────────────────────────

    /**
     * Streamer sends an SDP offer to start broadcasting.
     */
    private function handleOffer(WsServer $server, int $fd, string $tenantId, string $userId, array $data): void
    {
        $streamId = $data['stream_id'] ?? '';
        $sdp = $data['sdp'] ?? '';

        if ($streamId === '' || $sdp === '') {
            $server->push($fd, json_encode(['error' => 'Missing stream_id or sdp']));
            return;
        }

        // Validate stream key
        if (!$this->streamManager->validateStreamKey($tenantId, $streamId, $data['stream_key'] ?? '')) {
            $server->push($fd, json_encode(['error' => 'Invalid stream key']));
            return;
        }

        // Register this fd as the streamer
        $this->streamers[$streamId] = [
            'fd' => $fd,
            'tenant_id' => $tenantId,
            'user_id' => $userId,
        ];
        $this->fdToStream[$fd] = $streamId;

        if (!isset($this->viewers[$streamId])) {
            $this->viewers[$streamId] = [];
        }

        // Notify the streamer that broadcast is active
        $server->push($fd, json_encode([
            'type' => 'broadcast_started',
            'stream_id' => $streamId,
        ]));

        // Forward the offer to all existing viewers
        $this->notifyViewers($server, $streamId, [
            'type' => 'offer',
            'stream_id' => $streamId,
            'sdp' => $sdp,
        ]);

        $this->logger?->info('Stream broadcast started', [
            'stream_id' => $streamId,
            'tenant_id' => $tenantId,
            'user_id' => $userId,
        ]);
    }

    /**
     * Viewer sends an SDP answer back to the streamer.
     */
    private function handleAnswer(WsServer $server, int $fd, array $data): void
    {
        $streamId = $data['stream_id'] ?? '';
        $sdp = $data['sdp'] ?? '';

        if ($streamId === '' || $sdp === '') {
            $server->push($fd, json_encode(['error' => 'Missing stream_id or sdp']));
            return;
        }

        $streamer = $this->streamers[$streamId] ?? null;
        if ($streamer === null) {
            $server->push($fd, json_encode(['error' => 'Stream not found']));
            return;
        }

        // Relay the answer to the streamer
        if ($server->isEstablished($streamer['fd'])) {
            $server->push($streamer['fd'], json_encode([
                'type' => 'answer',
                'stream_id' => $streamId,
                'sdp' => $sdp,
                'viewer_fd' => $fd,
            ]));
        }
    }

    /**
     * Exchange ICE candidates between peers.
     */
    private function handleCandidate(WsServer $server, int $fd, array $data): void
    {
        $streamId = $data['stream_id'] ?? '';
        $candidate = $data['candidate'] ?? '';
        $targetFd = $data['target_fd'] ?? null;

        if ($streamId === '' || $candidate === '') {
            return; // Silently drop malformed ICE candidates
        }

        $message = json_encode([
            'type' => 'candidate',
            'stream_id' => $streamId,
            'candidate' => $candidate,
            'from_fd' => $fd,
        ]);

        if ($targetFd !== null) {
            // Targeted ICE candidate
            if ($server->isEstablished((int)$targetFd)) {
                $server->push((int)$targetFd, $message);
            }
        } else {
            // If from streamer → send to all viewers; if from viewer → send to streamer
            $streamer = $this->streamers[$streamId] ?? null;
            if ($streamer !== null && $streamer['fd'] === $fd) {
                // Streamer → all viewers
                $this->notifyViewers($server, $streamId, [
                    'type' => 'candidate',
                    'stream_id' => $streamId,
                    'candidate' => $candidate,
                    'from_fd' => $fd,
                ]);
            } elseif ($streamer !== null && $server->isEstablished($streamer['fd'])) {
                // Viewer → streamer
                $server->push($streamer['fd'], $message);
            }
        }
    }

    /**
     * Viewer subscribes to watch a stream.
     */
    private function handleSubscribe(WsServer $server, int $fd, string $tenantId, string $userId, array $data): void
    {
        $streamId = $data['stream_id'] ?? '';

        if ($streamId === '') {
            $server->push($fd, json_encode(['error' => 'Missing stream_id']));
            return;
        }

        if (!isset($this->viewers[$streamId])) {
            $this->viewers[$streamId] = [];
        }

        if (!in_array($fd, $this->viewers[$streamId], true)) {
            $this->viewers[$streamId][] = $fd;
        }
        $this->fdToStream[$fd] = $streamId;

        // If streamer is already broadcasting, send the current offer
        $streamer = $this->streamers[$streamId] ?? null;
        if ($streamer !== null) {
            $server->push($fd, json_encode([
                'type' => 'stream_active',
                'stream_id' => $streamId,
            ]));

            // Ask streamer to send a new offer to this viewer
            if ($server->isEstablished($streamer['fd'])) {
                $server->push($streamer['fd'], json_encode([
                    'type' => 'viewer_joined',
                    'stream_id' => $streamId,
                    'viewer_fd' => $fd,
                ]));
            }
        } else {
            $server->push($fd, json_encode([
                'type' => 'stream_waiting',
                'stream_id' => $streamId,
            ]));
        }

        $this->logger?->info('Viewer subscribed to stream', [
            'stream_id' => $streamId,
            'tenant_id' => $tenantId,
            'viewer_fd' => $fd,
        ]);
    }

    /**
     * Notify all viewers of a stream.
     */
    private function notifyViewers(WsServer $server, string $streamId, array $message): void
    {
        $viewers = $this->viewers[$streamId] ?? [];
        $payload = json_encode($message, JSON_THROW_ON_ERROR);

        foreach ($viewers as $fd) {
            if ($server->isEstablished($fd)) {
                $server->push($fd, $payload);
            }
        }
    }
}

