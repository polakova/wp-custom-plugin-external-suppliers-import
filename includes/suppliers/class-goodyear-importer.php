<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

require_once __DIR__ . '/../class-supplier-importer-base.php';
require_once __DIR__ . '/../class-ftp-handler.php';

/**
 * Goodyear supplier importer
 * Based on ext-stock-goodyear.php
 * Uses FTP URL (file_get_contents) to download CSV
 */
class Pneupex_Goodyear_Importer extends Pneupex_Supplier_Importer_Base {

	public function __construct() {
		parent::__construct( 'Goodyear' );
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

			// Use optimized bulk processing (migrated from custom_logic path)
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
	 */
	protected function download_file(): ?string {
		$url = defined( 'PNEUPEX_GOODYEAR_FTP_URL' ) ? PNEUPEX_GOODYEAR_FTP_URL : '';

		if ( empty( $url ) ) {
			// Fallback: construct URL from constants
			$host        = defined( 'PNEUPEX_GOODYEAR_FTP_HOST' ) ? PNEUPEX_GOODYEAR_FTP_HOST : 'ftp.goodyear.eu';
			$username    = defined( 'PNEUPEX_GOODYEAR_FTP_USER' ) ? PNEUPEX_GOODYEAR_FTP_USER : '';
			$password    = defined( 'PNEUPEX_GOODYEAR_FTP_PASS' ) ? PNEUPEX_GOODYEAR_FTP_PASS : '';
			$remote_file = defined( 'PNEUPEX_GOODYEAR_FTP_FILE' ) ? PNEUPEX_GOODYEAR_FTP_FILE : 'CONFIDENTIAL_GDYR_SK_STOCKREPORT_48h.csv';

			if ( empty( $username ) || empty( $password ) ) {
				$this->logger->error( 'FTP credentials not defined in constants.' );
				return null;
			}

			$url = "ftp://{$username}:{$password}@{$host}/{$remote_file}";
		}

		$ftp_handler = new Pneupex_FTP_Handler();
		return $ftp_handler->download_via_url( $url );
	}

	/**
	 * Parse CSV file
	 * Original: EAN in column 3, Stock (quantity) in column 5
	 * Original uses: fgetcsv($handle, 1000, ";") - no escape characters
	 */
	protected function parse_csv( string $file_path ): array {
		$products = array();

		if ( ( $handle = fopen( $file_path, 'r' ) ) !== false ) {
			// Match original: fgetcsv($handle, 1000, ";") - added escape parameter for PHP 8.4 compatibility
			while ( ( $row = fgetcsv( $handle, 1000, ';', '"', '\\' ) ) !== false ) {
				// EAN in column 3 (original: $data[3])
				if ( ! empty( $row[3] ) ) {
					$products[] = $row;
				}
			}
			fclose( $handle );
		}

		return $products;
	}

	protected function extract_sku( array $row ): string {
		return isset( $row[3] ) ? trim( (string) $row[3] ) : '';
	}

	protected function extract_quantity( array $row ): int {
		// Stock in column 5
		return isset( $row[5] ) ? (int) $row[5] : 0;
	}

	protected function extract_price( array $row ): float {
		return 0.0; // Uses product's own price (via get_base_price_from_product)
	}

	/**
	 * Override to get price from product instead of CSV
	 * Uses already-cached postmeta from update_postmeta_cache() call (no WC_Product object instantiation)
	 */
	protected function get_base_price_from_product( int $product_id, array $csv_row ): float {
		// Uses already-cached postmeta from update_postmeta_cache() call
		$price = get_post_meta( $product_id, '_price', true );
		if ( ! is_numeric( $price ) || (float) $price <= 0 ) {
			$this->logger->debug( "Skipping product {$product_id} - no price set." );
			return 0.0;
		}

		return (float) $price;
	}
}
