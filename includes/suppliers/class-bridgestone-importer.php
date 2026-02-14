<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

require_once __DIR__ . '/../class-supplier-importer-base.php';
require_once __DIR__ . '/../class-ftp-handler.php';

/**
 * Bridgestone supplier importer
 * Based on ext-stock-bridgestone.php
 * Uses SFTP (SSH2) to download CSV
 */
class Pneupex_Bridgestone_Importer extends Pneupex_Supplier_Importer_Base {

	public function __construct() {
		parent::__construct( 'Bridgestone' );
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
	 * Download file via SFTP
	 * Original: b-sftp.bridgestone.eu:3176, file: SSK1/Bridgestone_STOCKREPORT.csv
	 */
	protected function download_file(): ?string {
		$host     = defined( 'PNEUPEX_BRIDGESTONE_SFTP_HOST' ) ? PNEUPEX_BRIDGESTONE_SFTP_HOST : 'b-sftp.bridgestone.eu';
		$port     = defined( 'PNEUPEX_BRIDGESTONE_SFTP_PORT' ) ? PNEUPEX_BRIDGESTONE_SFTP_PORT : 3176;
		$username = defined( 'PNEUPEX_BRIDGESTONE_SFTP_USER' ) ? PNEUPEX_BRIDGESTONE_SFTP_USER : '';
		$password = defined( 'PNEUPEX_BRIDGESTONE_SFTP_PASS' ) ? PNEUPEX_BRIDGESTONE_SFTP_PASS : '';
		// Remote path should be full path (e.g., /SSK1/Bridgestone_STOCKREPORT.csv)
		$remote_path = defined( 'PNEUPEX_BRIDGESTONE_SFTP_FILE' ) ? PNEUPEX_BRIDGESTONE_SFTP_FILE : '/SSK1/Bridgestone_STOCKREPORT.csv';

		if ( empty( $username ) || empty( $password ) ) {
			$this->logger->error( 'SFTP credentials not defined in constants.' );
			return null;
		}

		$this->logger->info( "Connecting to SFTP: {$host}:{$port}" );
		$this->logger->info( "Remote file path: {$remote_path}" );

		$ftp_handler = new Pneupex_FTP_Handler();
		$result      = $ftp_handler->download_via_sftp( $host, $port, $username, $password, $remote_path );

		if ( ! $result ) {
			$this->logger->error( 'SFTP download failed. Check error_log for details.' );
		}

		return $result;
	}

	/**
	 * Parse CSV file
	 * Original: EAN in column 3, Stock (quantity) in column 6
	 */
	protected function parse_csv( string $file_path ): array {
		$products = array();

		if ( ( $handle = fopen( $file_path, 'r' ) ) !== false ) {
			while ( ( $row = fgetcsv( $handle, 1000, ';', '"', '\\' ) ) !== false ) {
				// Skip empty rows (col[0] != '')
				if ( ! empty( $row[0] ) && ! empty( $row[3] ) ) {
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
		// Stock in column 6
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
