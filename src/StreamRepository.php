<?php

declare(strict_types=1);

namespace Fabriq\Streaming;

use Fabriq\Storage\TenantAwareRepository;

/**
 * Persistent storage for stream records.
 *
 * Extends TenantAwareRepository to ensure all queries are tenant-scoped.
 * Stores stream metadata, history, and analytics in the app database.
 */
final class StreamRepository extends TenantAwareRepository
{
    /**
     * Save a stream record to the database.
     *
     * @param array{
     *     stream_id: string,
     *     tenant_id: string,
     *     user_id: string,
     *     title: string,
     *     status: string,
     *     started_at: ?int,
     *     ended_at: ?int,
     *     metadata: array<string, mixed>,
     * } $stream
     */
    public function save(array $stream): void
    {
        $this->withAppDb(function ($conn) use ($stream) {
            $insert = $this->buildInsert('streams', [
                'stream_id' => $stream['stream_id'],
                'tenant_id' => $this->tenantId(),
                'user_id' => $stream['user_id'],
                'title' => $stream['title'],
                'status' => $stream['status'],
                'started_at' => $stream['started_at'],
                'ended_at' => $stream['ended_at'],
                'metadata' => json_encode($stream['metadata'] ?? [], JSON_THROW_ON_ERROR),
                'created_at' => time(),
            ]);

            $this->execute($conn, $insert['sql'], $insert['params']);
        });
    }

    /**
     * Update stream status.
     */
    public function updateStatus(string $streamId, string $status, ?int $timestamp = null): void
    {
        $this->withAppDb(function ($conn) use ($streamId, $status, $timestamp) {
            $sql = 'UPDATE streams SET status = ?, ended_at = ? WHERE stream_id = ? AND tenant_id = ?';
            $this->execute($conn, $sql, [$status, $timestamp, $streamId, $this->tenantId()]);
        });
    }

    /**
     * Find a stream by ID.
     *
     * @return array<string, mixed>|null
     */
    public function find(string $streamId): ?array
    {
        return $this->withAppDb(function ($conn) use ($streamId) {
            $sql = 'SELECT * FROM streams WHERE stream_id = ? AND tenant_id = ? LIMIT 1';
            $result = $this->execute($conn, $sql, [$streamId, $this->tenantId()]);

            if ($result === true || $result === false) {
                return null;
            }

            $row = $result->fetch();
            return is_array($row) ? $row : null;
        });
    }

    /**
     * List recent streams for the current tenant.
     *
     * @return list<array<string, mixed>>
     */
    public function recent(int $limit = 20): array
    {
        return $this->withAppDb(function ($conn) use ($limit) {
            $sql = 'SELECT * FROM streams WHERE tenant_id = ? ORDER BY created_at DESC LIMIT ?';
            $result = $this->execute($conn, $sql, [$this->tenantId(), $limit]);

            if ($result === true || $result === false) {
                return [];
            }

            $rows = [];
            while ($row = $result->fetch()) {
                if (is_array($row)) {
                    $rows[] = $row;
                }
            }
            return $rows;
        });
    }
}

