<?php

declare(strict_types=1);

namespace Fabriq\Streaming;

use Fabriq\Kernel\Config;
use Fabriq\Observability\Logger;
use Swoole\Coroutine;
use Swoole\Process;

/**
 * FFmpeg transcoding process manager.
 *
 * Spawns and manages FFmpeg processes that transcode incoming streams
 * (RTMP/WHIP/WebRTC) into HLS output (`.m3u8` + `.ts` segments).
 *
 * For large audiences (>50 viewers), this replaces P2P with HLS:
 *   - FFmpeg receives the stream as input
 *   - Outputs HLS segments to the configured storage path
 *   - HlsManager serves the manifest and segments via HTTP
 *   - Thousands of viewers consume HLS (cacheable by CDN)
 *
 * Controls:
 *   - Max concurrent transcodes (to limit CPU/memory)
 *   - Automatic cleanup on stream end
 *   - Process health monitoring
 */
final class TranscodingPipeline
{
    /** @var array<string, array{process: Process|null, pid: int, started_at: int}> streamId → process info */
    private array $processes = [];

    private string $ffmpegPath;
    private string $storagePath;
    private int $segmentDuration;
    private int $playlistSize;
    private int $maxConcurrent;

    public function __construct(
        private readonly ?Config $config = null,
        private readonly ?Logger $logger = null,
    ) {
        $this->ffmpegPath = (string)($config?->get('streaming.ffmpeg_path', '/usr/bin/ffmpeg') ?? '/usr/bin/ffmpeg');
        $this->storagePath = (string)($config?->get('streaming.hls.storage_path', '/tmp/fabriq-hls') ?? '/tmp/fabriq-hls');
        $this->segmentDuration = (int)($config?->get('streaming.hls.segment_duration', 4) ?? 4);
        $this->playlistSize = (int)($config?->get('streaming.hls.playlist_size', 5) ?? 5);
        $this->maxConcurrent = (int)($config?->get('streaming.max_concurrent_transcodes', 4) ?? 4);
    }

    /**
     * Start transcoding a stream to HLS.
     *
     * @param string $streamId Unique stream identifier
     * @param string $inputUrl Input stream URL (e.g., rtmp://..., pipe:0, or SDP file)
     * @return bool True if transcoding started successfully
     */
    public function start(string $streamId, string $inputUrl): bool
    {
        if (isset($this->processes[$streamId])) {
            $this->logger?->warning('Transcoding already active for stream', ['stream_id' => $streamId]);
            return false;
        }

        if (count($this->processes) >= $this->maxConcurrent) {
            $this->logger?->warning('Max concurrent transcodes reached', [
                'current' => count($this->processes),
                'max' => $this->maxConcurrent,
            ]);
            return false;
        }

        // Ensure output directory exists
        $outputDir = $this->getStreamOutputDir($streamId);
        if (!is_dir($outputDir)) {
            mkdir($outputDir, 0755, true);
        }

        $manifestPath = $outputDir . '/playlist.m3u8';
        $segmentPattern = $outputDir . '/segment_%05d.ts';

        // Build FFmpeg command
        $command = $this->buildFfmpegCommand($inputUrl, $manifestPath, $segmentPattern);

        $this->logger?->info('Starting FFmpeg transcoding', [
            'stream_id' => $streamId,
            'command' => $command,
        ]);

        // Spawn FFmpeg as a Swoole Process
        $process = new Process(function (Process $proc) use ($command) {
            $proc->exec('/bin/sh', ['-c', $command]);
        }, false, 0, true);

        $pid = $process->start();
        if ($pid === false) {
            $this->logger?->error('Failed to start FFmpeg process', ['stream_id' => $streamId]);
            return false;
        }

        $this->processes[$streamId] = [
            'process' => $process,
            'pid' => $pid,
            'started_at' => time(),
        ];

        $this->logger?->info('FFmpeg transcoding started', [
            'stream_id' => $streamId,
            'pid' => $pid,
        ]);

        return true;
    }

    /**
     * Stop transcoding a stream.
     */
    public function stop(string $streamId): bool
    {
        if (!isset($this->processes[$streamId])) {
            return false;
        }

        $info = $this->processes[$streamId];

        // Send SIGTERM to FFmpeg
        if ($info['pid'] > 0) {
            Process::kill($info['pid'], SIGTERM);

            // Give it 3 seconds to gracefully shut down, then SIGKILL
            Coroutine::create(function () use ($info, $streamId) {
                Coroutine::sleep(3.0);
                if (Process::kill($info['pid'], 0)) {
                    Process::kill($info['pid'], SIGKILL);
                    $this->logger?->warning('FFmpeg force-killed', [
                        'stream_id' => $streamId,
                        'pid' => $info['pid'],
                    ]);
                }
            });
        }

        unset($this->processes[$streamId]);

        $this->logger?->info('FFmpeg transcoding stopped', [
            'stream_id' => $streamId,
        ]);

        return true;
    }

    /**
     * Check if a stream is being transcoded.
     */
    public function isActive(string $streamId): bool
    {
        if (!isset($this->processes[$streamId])) {
            return false;
        }

        // Verify the process is still running
        $pid = $this->processes[$streamId]['pid'];
        if ($pid > 0 && !Process::kill($pid, 0)) {
            // Process has exited
            unset($this->processes[$streamId]);
            return false;
        }

        return true;
    }

    /**
     * Get the output directory for a stream's HLS files.
     */
    public function getStreamOutputDir(string $streamId): string
    {
        return $this->storagePath . '/' . $streamId;
    }

    /**
     * Get the manifest path for a stream.
     */
    public function getManifestPath(string $streamId): string
    {
        return $this->getStreamOutputDir($streamId) . '/playlist.m3u8';
    }

    /**
     * Clean up HLS files for a stream.
     */
    public function cleanup(string $streamId): void
    {
        $dir = $this->getStreamOutputDir($streamId);
        if (!is_dir($dir)) {
            return;
        }

        $files = glob($dir . '/*');
        if (is_array($files)) {
            foreach ($files as $file) {
                if (is_file($file)) {
                    unlink($file);
                }
            }
        }

        rmdir($dir);

        $this->logger?->info('HLS files cleaned up', ['stream_id' => $streamId]);
    }

    /**
     * Stop all transcoding processes.
     */
    public function stopAll(): void
    {
        foreach (array_keys($this->processes) as $streamId) {
            $this->stop($streamId);
        }
    }

    /**
     * Get transcoding stats.
     *
     * @return array{active: int, max: int, streams: list<string>}
     */
    public function stats(): array
    {
        return [
            'active' => count($this->processes),
            'max' => $this->maxConcurrent,
            'streams' => array_keys($this->processes),
        ];
    }

    // ── Internals ───────────────────────────────────────────────────

    /**
     * Build the FFmpeg command for HLS transcoding.
     */
    private function buildFfmpegCommand(string $input, string $manifestPath, string $segmentPattern): string
    {
        return sprintf(
            '%s -y -i %s '
            . '-c:v libx264 -preset veryfast -tune zerolatency -crf 23 '
            . '-c:a aac -b:a 128k -ar 44100 '
            . '-f hls -hls_time %d -hls_list_size %d '
            . '-hls_flags delete_segments+append_list '
            . '-hls_segment_filename %s %s '
            . '2>&1',
            escapeshellarg($this->ffmpegPath),
            escapeshellarg($input),
            $this->segmentDuration,
            $this->playlistSize,
            escapeshellarg($segmentPattern),
            escapeshellarg($manifestPath),
        );
    }
}

