<?php

declare(strict_types=1);

namespace Fabriq\Streaming;

use Fabriq\Kernel\Config;
use Swoole\Http\Request;
use Swoole\Http\Response;

/**
 * HLS manifest and segment server.
 *
 * Serves `.m3u8` playlists and `.ts` video segments via HTTP.
 * Works with TranscodingPipeline which writes HLS output to disk.
 *
 * URL patterns:
 *   GET /hls/{streamId}/playlist.m3u8  → HLS manifest
 *   GET /hls/{streamId}/{segment}.ts   → Video segment
 *
 * Designed to sit behind a CDN for massive scalability.
 * Thousands of viewers can consume HLS with minimal server load.
 */
final class HlsManager
{
    private string $storagePath;

    /** @var array<string, string> Extension → Content-Type */
    private const CONTENT_TYPES = [
        'm3u8' => 'application/vnd.apple.mpegurl',
        'ts' => 'video/mp2t',
    ];

    public function __construct(
        private readonly ?Config $config = null,
    ) {
        $this->storagePath = (string)($config?->get('streaming.hls.storage_path', '/tmp/fabriq-hls') ?? '/tmp/fabriq-hls');
    }

    /**
     * Handle an HLS HTTP request.
     *
     * @param Request $request
     * @param Response $response
     * @param string $streamId
     * @param string $filename The requested file (e.g., "playlist.m3u8" or "segment_00001.ts")
     * @return bool True if the request was handled
     */
    public function serve(Request $request, Response $response, string $streamId, string $filename): bool
    {
        // Validate filename to prevent path traversal
        if (str_contains($filename, '..') || str_contains($filename, '/') || str_contains($filename, '\\')) {
            $response->status(400);
            $response->end('Invalid filename');
            return true;
        }

        $filePath = $this->storagePath . '/' . $streamId . '/' . $filename;

        if (!is_file($filePath)) {
            $response->status(404);
            $response->header('Content-Type', 'application/json');
            $response->end(json_encode(['error' => 'Segment not found']));
            return true;
        }

        // Determine content type
        $ext = pathinfo($filename, PATHINFO_EXTENSION);
        $contentType = self::CONTENT_TYPES[$ext] ?? 'application/octet-stream';

        // Set CORS headers for browser playback
        $response->header('Content-Type', $contentType);
        $response->header('Access-Control-Allow-Origin', '*');
        $response->header('Access-Control-Allow-Methods', 'GET, OPTIONS');
        $response->header('Access-Control-Allow-Headers', 'Content-Type');

        // Cache control — segments are immutable, manifests are dynamic
        if ($ext === 'm3u8') {
            $response->header('Cache-Control', 'no-cache, no-store, must-revalidate');
        } else {
            $response->header('Cache-Control', 'public, max-age=31536000, immutable');
        }

        $response->sendfile($filePath);

        return true;
    }

    /**
     * Check if HLS output exists for a stream.
     */
    public function hasManifest(string $streamId): bool
    {
        return is_file($this->storagePath . '/' . $streamId . '/playlist.m3u8');
    }

    /**
     * Get the HLS playback URL for a stream.
     *
     * @param string $streamId
     * @param string $baseUrl The base URL of the server
     * @return string Full playback URL
     */
    public function getPlaybackUrl(string $streamId, string $baseUrl = ''): string
    {
        return rtrim($baseUrl, '/') . "/hls/{$streamId}/playlist.m3u8";
    }

    /**
     * List all available segments for a stream.
     *
     * @return list<string>
     */
    public function listSegments(string $streamId): array
    {
        $dir = $this->storagePath . '/' . $streamId;
        if (!is_dir($dir)) {
            return [];
        }

        $files = glob($dir . '/*.ts');
        return is_array($files) ? array_map('basename', $files) : [];
    }

    /**
     * Get storage stats.
     *
     * @return array{storage_path: string, streams_with_hls: int}
     */
    public function stats(): array
    {
        $count = 0;
        if (is_dir($this->storagePath)) {
            $dirs = glob($this->storagePath . '/*/playlist.m3u8');
            $count = is_array($dirs) ? count($dirs) : 0;
        }

        return [
            'storage_path' => $this->storagePath,
            'streams_with_hls' => $count,
        ];
    }
}

