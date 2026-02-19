<?php

declare(strict_types=1);

namespace Fabriq\Streaming;

use Fabriq\Kernel\Config;
use Fabriq\Storage\DbManager;

/**
 * Chat moderation for live streams.
 *
 * Features:
 *   - Slow mode: enforce minimum delay between messages
 *   - Max message length
 *   - Word filter: block messages containing banned words
 *   - Subscriber-only mode
 *   - Ban list: block specific users from chatting
 *
 * All state is stored in Redis for cross-worker consistency.
 */
final class ChatModerator
{
    private int $slowModeSeconds;
    private int $maxMessageLength;

    public function __construct(
        private readonly DbManager $db,
        private readonly ?Config $config = null,
    ) {
        $this->slowModeSeconds = (int)($config?->get('streaming.chat.slow_mode_seconds', 0) ?? 0);
        $this->maxMessageLength = (int)($config?->get('streaming.chat.max_message_length', 500) ?? 500);
    }

    /**
     * Validate a chat message before sending.
     *
     * @param string $tenantId
     * @param string $streamId
     * @param string $userId
     * @param string $message
     * @return array{allowed: bool, reason: string}
     */
    public function validate(string $tenantId, string $streamId, string $userId, string $message): array
    {
        // Check message length
        if (mb_strlen($message) > $this->maxMessageLength) {
            return ['allowed' => false, 'reason' => "Message exceeds {$this->maxMessageLength} characters"];
        }

        // Check if empty
        if (trim($message) === '') {
            return ['allowed' => false, 'reason' => 'Message cannot be empty'];
        }

        // Check ban list
        if ($this->isBanned($tenantId, $streamId, $userId)) {
            return ['allowed' => false, 'reason' => 'You are banned from this chat'];
        }

        // Check word filter
        $bannedWord = $this->containsBannedWord($tenantId, $streamId, $message);
        if ($bannedWord !== null) {
            return ['allowed' => false, 'reason' => 'Message contains prohibited content'];
        }

        // Check slow mode
        if ($this->slowModeSeconds > 0 && !$this->checkSlowMode($tenantId, $streamId, $userId)) {
            return ['allowed' => false, 'reason' => "Slow mode: wait {$this->slowModeSeconds}s between messages"];
        }

        return ['allowed' => true, 'reason' => ''];
    }

    /**
     * Ban a user from chatting in a stream.
     */
    public function ban(string $tenantId, string $streamId, string $userId, int $durationSeconds = 0): void
    {
        $key = "chat_ban:{$tenantId}:{$streamId}";

        $redis = $this->db->redis();
        try {
            $redis->sAdd($key, $userId);
            if ($durationSeconds > 0) {
                $redis->expire($key, $durationSeconds);
            }
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    /**
     * Unban a user.
     */
    public function unban(string $tenantId, string $streamId, string $userId): void
    {
        $key = "chat_ban:{$tenantId}:{$streamId}";

        $redis = $this->db->redis();
        try {
            $redis->sRem($key, $userId);
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    /**
     * Add words to the filter list for a stream.
     *
     * @param list<string> $words
     */
    public function addFilteredWords(string $tenantId, string $streamId, array $words): void
    {
        $key = "chat_filter:{$tenantId}:{$streamId}";

        $redis = $this->db->redis();
        try {
            foreach ($words as $word) {
                $redis->sAdd($key, mb_strtolower($word));
            }
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    /**
     * Set slow mode for a stream (0 to disable).
     */
    public function setSlowMode(int $seconds): void
    {
        $this->slowModeSeconds = max(0, $seconds);
    }

    // ── Internals ───────────────────────────────────────────────────

    private function isBanned(string $tenantId, string $streamId, string $userId): bool
    {
        $key = "chat_ban:{$tenantId}:{$streamId}";

        $redis = $this->db->redis();
        try {
            return (bool)$redis->sIsMember($key, $userId);
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    private function containsBannedWord(string $tenantId, string $streamId, string $message): ?string
    {
        $key = "chat_filter:{$tenantId}:{$streamId}";

        $redis = $this->db->redis();
        try {
            $words = $redis->sMembers($key);
            if (!is_array($words) || count($words) === 0) {
                return null;
            }

            $lower = mb_strtolower($message);
            foreach ($words as $word) {
                if (str_contains($lower, $word)) {
                    return $word;
                }
            }

            return null;
        } finally {
            $this->db->releaseRedis($redis);
        }
    }

    private function checkSlowMode(string $tenantId, string $streamId, string $userId): bool
    {
        $key = "chat_slow:{$tenantId}:{$streamId}:{$userId}";

        $redis = $this->db->redis();
        try {
            // Try to set the key — if it already exists, slow mode is active
            $set = $redis->set($key, '1', ['NX', 'EX' => $this->slowModeSeconds]);
            return $set !== false;
        } finally {
            $this->db->releaseRedis($redis);
        }
    }
}

