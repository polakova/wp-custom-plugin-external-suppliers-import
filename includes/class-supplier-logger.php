<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

/**
 * Custom logging system for supplier imports
 */
class Pneupex_Supplier_Logger {

	private string $log_dir;
	private string $supplier_name;
	/** @var resource|null */
	private $log_file             = null;
	private string $log_file_path = '';

	public function __construct( string $supplier_name ) {
		$this->supplier_name = sanitize_file_name( $supplier_name );

		// Ensure WordPress is loaded before calling wp_upload_dir()
		if ( ! function_exists( 'wp_upload_dir' ) ) {
			$this->log_dir = WP_CONTENT_DIR . '/uploads/pneupex-supplier-logs/';
		} else {
			$upload_dir    = wp_upload_dir();
			$this->log_dir = trailingslashit( $upload_dir['basedir'] ) . 'pneupex-supplier-logs/';
		}

		if ( ! file_exists( $this->log_dir ) ) {
			if ( function_exists( 'wp_mkdir_p' ) ) {
				wp_mkdir_p( $this->log_dir );
			} else {
				@mkdir( $this->log_dir, 0755, true );
			}
		}
	}

	/**
	 * Start a new log session
	 */
	public function start_log(): void {
		$timestamp           = date( 'Y-m-d_H-i-s' );
		$this->log_file_path = $this->log_dir . $this->supplier_name . '_' . $timestamp . '.log';
		$this->log_file      = fopen( $this->log_file_path, 'a' );

		if ( $this->log_file ) {
			$this->write( 'INFO', "=== Import started for supplier: {$this->supplier_name} ===" );
		}
	}

	/**
	 * End log session
	 */
	public function end_log(): void {
		if ( $this->log_file ) {
			$this->write( 'INFO', "=== Import finished for supplier: {$this->supplier_name} ===" );
			fclose( $this->log_file );
			$this->log_file = null;
		}
	}

	/**
	 * Write log entry
	 */
	private function write( string $level, string $message ): void {
		$timestamp = date( 'Y-m-d H:i:s' );
		$log_entry = "[{$timestamp}] [{$level}] {$message}" . PHP_EOL;

		if ( $this->log_file ) {
			fwrite( $this->log_file, $log_entry );
		}
	}

	/**
	 * Log info message
	 */
	public function info( string $message ): void {
		$this->write( 'INFO', $message );
	}

	/**
	 * Log error message
	 */
	public function error( string $message ): void {
		$this->write( 'ERROR', $message );
	}

	/**
	 * Log warning message
	 */
	public function warning( string $message ): void {
		$this->write( 'WARNING', $message );
	}

	/**
	 * Log debug message
	 */
	public function debug( string $message ): void {
		$this->write( 'DEBUG', $message );
	}

	/**
	 * Get log file path
	 */
	public function get_log_file_path(): string {
		return $this->log_file_path;
	}

	/**
	 * Close log file (public method for external closing)
	 */
	public function close_log(): void {
		$this->end_log();
	}

	/**
	 * Check if log file is currently open
	 */
	public function is_log_open(): bool {
		return $this->log_file !== null;
	}

	/**
	 * Get all log files for this supplier
	 */
	public function get_log_files(): array {
		$files = array();
		if ( ! is_dir( $this->log_dir ) ) {
			return $files;
		}

		$pattern = $this->log_dir . $this->supplier_name . '_*.log';
		$matches = glob( $pattern );

		if ( $matches ) {
			rsort( $matches ); // Newest first
			foreach ( $matches as $file ) {
				$files[] = array(
					'path' => $file,
					'name' => basename( $file ),
					'size' => filesize( $file ),
					'date' => filemtime( $file ),
				);
			}
		}

		return $files;
	}
}
