<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

/**
 * Manages cleanup of temp files and old logs
 */
class Pneupex_Cleanup_Manager {

	/**
	 * Clean up old temp files (older than 1 hour)
	 */
	public static function cleanup_temp_files(): void {
		if ( ! function_exists( 'wp_upload_dir' ) ) {
			return; // WordPress not fully loaded
		}

		$upload_dir = wp_upload_dir();
		if ( ! $upload_dir || isset( $upload_dir['error'] ) ) {
			return; // Upload dir not available
		}

		$temp_dir = trailingslashit( $upload_dir['basedir'] ) . 'pneupex-temp/';

		if ( ! is_dir( $temp_dir ) ) {
			return;
		}

		$files = @glob( $temp_dir . '*' );
		if ( $files === false || empty( $files ) ) {
			return;
		}

		$deleted      = 0;
		$one_hour_ago = time() - 3600;

		foreach ( $files as $file ) {
			if ( is_file( $file ) ) {
				$file_time = @filemtime( $file );
				if ( $file_time && $file_time < $one_hour_ago ) {
					@unlink( $file );
					++$deleted;
				}
			}
		}

		if ( $deleted > 0 ) {
			error_log( "[PNEUPEX_CLEANUP] Deleted {$deleted} old temp files." );
		}
	}

	/**
	 * Clean up old log files (older than 7 days by default)
	 */
	public static function cleanup_log_files( int $days = 7 ): void {
		if ( ! function_exists( 'wp_upload_dir' ) ) {
			return; // WordPress not fully loaded
		}

		$upload_dir = wp_upload_dir();
		if ( ! $upload_dir || isset( $upload_dir['error'] ) ) {
			return; // Upload dir not available
		}

		$log_dir = trailingslashit( $upload_dir['basedir'] ) . 'pneupex-supplier-logs/';

		if ( ! is_dir( $log_dir ) ) {
			return;
		}

		$files = @glob( $log_dir . '*.log' );
		if ( $files === false || empty( $files ) ) {
			return;
		}

		$deleted     = 0;
		$cutoff_time = time() - ( $days * 24 * 3600 );

		foreach ( $files as $file ) {
			if ( is_file( $file ) ) {
				$file_time = @filemtime( $file );
				if ( $file_time && $file_time < $cutoff_time ) {
					@unlink( $file );
					++$deleted;
				}
			}
		}

		if ( $deleted > 0 ) {
			error_log( "[PNEUPEX_CLEANUP] Deleted {$deleted} old log files (older than {$days} days)." );
		}
	}

	/**
	 * Run all cleanup tasks
	 */
	public static function run_cleanup(): void {
		self::cleanup_temp_files();
		self::cleanup_log_files();
	}
}
