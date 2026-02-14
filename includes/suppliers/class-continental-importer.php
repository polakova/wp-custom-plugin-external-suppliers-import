<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

require_once __DIR__ . '/../class-supplier-importer-base.php';
require_once __DIR__ . '/../class-ftp-handler.php';

/**
 * Continental supplier importer
 * Based on ext-stock-continental.php
 */
class Pneupex_Continental_Importer extends Pneupex_Supplier_Importer_Base {

	public function __construct() {
		parent::__construct( 'Continental' );
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
	 * Download file via FTP
	 */
	protected function download_file(): ?string {
		$host     = defined( 'PNEUPEX_CONTINENTAL_FTP_HOST' ) ? PNEUPEX_CONTINENTAL_FTP_HOST : 'ftp.pexstore.sk';
		$username = defined( 'PNEUPEX_CONTINENTAL_FTP_USER' ) ? PNEUPEX_CONTINENTAL_FTP_USER : '';
		$password = defined( 'PNEUPEX_CONTINENTAL_FTP_PASS' ) ? PNEUPEX_CONTINENTAL_FTP_PASS : '';
		$base_dir = defined( 'PNEUPEX_CONTINENTAL_FTP_DIR' ) ? PNEUPEX_CONTINENTAL_FTP_DIR : 'continental';

		if ( empty( $host ) || empty( $username ) || empty( $password ) ) {
			$this->logger->error( 'FTP credentials not defined in constants.' );
			return null;
		}

		$this->logger->info( "Attempting FTP connection to: {$host}" );

		$ftp_handler = new Pneupex_FTP_Handler();
		$local_file  = $ftp_handler->download_via_ftp_complete( $host, $username, $password, $base_dir );

		if ( $local_file ) {
			$file_size = filesize( $local_file );
			$this->logger->info( "Successfully downloaded file to: {$local_file} (Size: {$file_size} bytes)" );
		} else {
			$this->logger->error( "Failed to download file from FTP server: {$host}" );
		}

		return $local_file;
	}

	/**
	 * Parse CSV file
	 * Original: Skip first 3 rows, EAN in column 3, Quantity in column 6
	 */
	protected function parse_csv( string $file_path ): array {
		$products = array();
		$line     = 0;

		if ( ( $handle = fopen( $file_path, 'r' ) ) !== false ) {
			while ( ( $row = fgetcsv( $handle, 1000, ';', '"', '\\' ) ) !== false ) {
				if ( $line++ < 3 ) {
					continue;
				}

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
		return isset( $row[6] ) ? (int) $row[6] : 0;
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
