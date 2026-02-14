<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

require_once __DIR__ . '/../class-supplier-importer-base.php';
require_once __DIR__ . '/../class-ftp-handler.php';

/**
 * Vredestein supplier importer
 * Based on ext-stock-vredestein.php
 * Uses FTP URL to download CSV
 * EAN in col[0], Price in col[4], Quantity in col[1]
 */
class Pneupex_Vredestein_Importer extends Pneupex_Supplier_Importer_Base {

	public function __construct() {
		parent::__construct( 'Vredestein' );
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
				$this->logger->error( 'Failed to download file from FTP.' );
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
	 * Download file via FTP URL
	 * Original: ftp://mh040401:...@ftp.pneupex.sk/vredestein/Vredestein.csv
	 */
	protected function download_file(): ?string {
		$host        = defined( 'PNEUPEX_VREDESTEIN_FTP_HOST' ) ? PNEUPEX_VREDESTEIN_FTP_HOST : 'ftp.pneupex.sk';
		$username    = defined( 'PNEUPEX_VREDESTEIN_FTP_USER' ) ? PNEUPEX_VREDESTEIN_FTP_USER : '';
		$password    = defined( 'PNEUPEX_VREDESTEIN_FTP_PASS' ) ? PNEUPEX_VREDESTEIN_FTP_PASS : '';
		$remote_file = defined( 'PNEUPEX_VREDESTEIN_FTP_FILE' ) ? PNEUPEX_VREDESTEIN_FTP_FILE : 'vredestein/Vredestein.csv';

		if ( empty( $username ) || empty( $password ) ) {
			$this->logger->error( 'FTP credentials not defined in constants.' );
			return null;
		}

		$url = "ftp://{$username}:{$password}@{$host}/{$remote_file}";

		$ftp_handler = new Pneupex_FTP_Handler();
		return $ftp_handler->download_via_url( $url );
	}

	/**
	 * Parse CSV file
	 * Original: EAN in col[0], Price in col[4], Quantity in col[1]
	 */
	protected function parse_csv( string $file_path ): array {
		$products = array();

		if ( ( $handle = fopen( $file_path, 'r' ) ) !== false ) {
			while ( ( $row = fgetcsv( $handle, 1000, ';', '"', '\\' ) ) !== false ) {
				if ( ! empty( $row[0] ) ) {
					$products[] = $row;
				}
			}
			fclose( $handle );
		}

		return $products;
	}

	protected function extract_sku( array $row ): string {
		return isset( $row[0] ) ? trim( (string) $row[0] ) : '';
	}

	protected function extract_quantity( array $row ): int {
		// Quantity in col[1]
		return isset( $row[1] ) ? (int) $row[1] : 0;
	}

	protected function extract_price( array $row ): float {
		// Price in col[4]
		return isset( $row[4] ) ? (float) $row[4] : 0.0;
	}
}
