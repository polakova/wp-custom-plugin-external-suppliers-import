<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

require_once __DIR__ . '/../class-supplier-importer-base.php';
require_once __DIR__ . '/../class-ftp-handler.php';

/**
 * Alcar supplier importer
 * Based on ext-stock-alcar.php
 * Uses SFTP (SSH2) to download CSV
 * EAN in col[4], Price in col[5] (with comma), Quantity = col[1] + col[2]
 */
class Pneupex_Alcar_Importer extends Pneupex_Supplier_Importer_Base {

	public function __construct() {
		parent::__construct( 'Alcar' );
	}

	/**
	 * Main import method
	 */
	public function import( ?int $row_limit = null ): array {
		$this->logger->start_log();

		$stats = array(
			'processed' => 0,
			'updated'   => 0,
			'errors'    => 0,
			'skipped'   => 0,
		);

		try {
			$file_path = $this->download_file();
			if ( ! $file_path ) {
				$this->logger->error( 'Failed to download file from SFTP.' );
				$this->logger->end_log();
				return $stats;
			}

			$this->logger->info( "File downloaded: {$file_path}" );

			$products = $this->parse_csv( $file_path );
			$this->logger->info( 'Found ' . count( $products ) . ' products in CSV.' );

			// Apply row limit for testing (if specified)
			$products = $this->apply_row_limit( $products, $row_limit );

			// Use optimized batch processing with chunked memory management
			$chunk_size  = apply_filters( 'pneupex_import_chunk_size', 50 );
			$chunk_stats = $this->process_products_chunked( $products, $chunk_size );

			// Merge chunk stats into main stats
			$stats['processed'] += $chunk_stats['processed'];
			$stats['updated']   += $chunk_stats['updated'];
			$stats['errors']    += $chunk_stats['errors'];
			$stats['skipped']   += $chunk_stats['skipped'];

			$ftp_handler = new Pneupex_FTP_Handler();
			$ftp_handler->cleanup( $file_path );

		} catch ( \Exception $e ) {
			$this->logger->error( 'Import failed: ' . $e->getMessage() );
			++$stats['errors'];
		}

		$this->logger->info( "Import completed. Processed: {$stats['processed']}, Errors: {$stats['errors']}." );

		return $stats;
	}

	/**
	 * Download file via SFTP
	 * Original: ftp.alcar-wheels.com:22, file: SK_R_00.csv
	 */
	protected function download_file(): ?string {
		$host        = defined( 'PNEUPEX_ALCAR_SFTP_HOST' ) ? PNEUPEX_ALCAR_SFTP_HOST : 'ftp.alcar-wheels.com';
		$port        = defined( 'PNEUPEX_ALCAR_SFTP_PORT' ) ? PNEUPEX_ALCAR_SFTP_PORT : 22;
		$username    = defined( 'PNEUPEX_ALCAR_SFTP_USER' ) ? PNEUPEX_ALCAR_SFTP_USER : '';
		$password    = defined( 'PNEUPEX_ALCAR_SFTP_PASS' ) ? PNEUPEX_ALCAR_SFTP_PASS : '';
		$remote_path = defined( 'PNEUPEX_ALCAR_SFTP_FILE' ) ? PNEUPEX_ALCAR_SFTP_FILE : 'SK_R_00.csv';

		if ( empty( $username ) || empty( $password ) ) {
			$this->logger->error( 'SFTP credentials not defined in constants.' );
			return null;
		}

		$ftp_handler = new Pneupex_FTP_Handler();
		return $ftp_handler->download_via_sftp( $host, $port, $username, $password, $remote_path );
	}

	/**
	 * Parse CSV file
	 * Original: EAN in col[4]
	 */
	protected function parse_csv( string $file_path ): array {
		$products = array();

		if ( ( $handle = fopen( $file_path, 'r' ) ) !== false ) {
			while ( ( $row = fgetcsv( $handle, 1000, ';', '"', '\\' ) ) !== false ) {
				if ( ! empty( $row[4] ) ) {
					$products[] = $row;
				}
			}
			fclose( $handle );
		}

		return $products;
	}

	protected function extract_sku( array $row ): string {
		return isset( $row[4] ) ? trim( (string) $row[4] ) : '';
	}

	protected function extract_quantity( array $row ): int {
		// Quantity = col[1] + col[2]
		$qty1 = isset( $row[1] ) ? (int) $row[1] : 0;
		$qty2 = isset( $row[2] ) ? (int) $row[2] : 0;
		return $qty1 + $qty2;
	}

	protected function extract_price( array $row ): float {
		// Price in col[5], replace comma with dot
		if ( ! empty( $row[5] ) ) {
			return (float) str_replace( ',', '.', $row[5] );
		}
		return 0.0;
	}
}
