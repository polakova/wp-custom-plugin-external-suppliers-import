<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

require_once __DIR__ . '/../class-supplier-importer-base.php';
require_once __DIR__ . '/../class-ftp-handler.php';

/**
 * Latex supplier importer
 * Based on importlatex.php (Eshop_Product_Importlatex)
 * Uses HTTP URL to download CSV file
 *
 * CSV structure (matches original constants):
 * - COL_ONSTOCK = 4
 * - COL_PRICE = 9
 * - COL_EAN = 13
 * - COL_AGE = 29
 * - COL_ISDEMO = 38
 * - COL_DOT = 35
 *
 * Logic from original:
 * - Skip first row (header)
 * - Skip rows where EAN is empty or '0', or price is 0, or AGE is 'STARE DOTY'
 * - Process DOT: if string, take substring from position 3
 * - Check ISDEMO == "DEMO DM"
 */
class Pneupex_Latex_Importer extends Pneupex_Supplier_Importer_Base {

	/**
	 * CSV column indices (matches Eshop_Product_Importlatex constants)
	 */
	private const COL_ONSTOCK = 4;
	private const COL_PRICE   = 9;
	private const COL_EAN     = 13;
	private const COL_AGE     = 29;
	private const COL_ISDEMO  = 38;
	private const COL_DOT     = 35;

	public function __construct() {
		parent::__construct( 'Latex' );
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
				$this->logger->error( 'Failed to download file from URL.' );
				$this->logger->end_log();
				return $stats;
			}

			$this->logger->info( "File downloaded: {$file_path}" );

			$products = $this->parse_csv( $file_path );
			$this->logger->info( 'Found ' . count( $products ) . ' products in CSV.' );

			// Apply row limit for testing (if specified)
			$products = $this->apply_row_limit( $products, $row_limit );

			// Filter out rows that should be skipped before chunked processing
			$valid_products = array();
			foreach ( $products as $row ) {
				if ( ! $this->should_skip_row( $row ) ) {
					$valid_products[] = $row;
				} else {
					++$stats['skipped'];
				}
			}

			// Use optimized bulk processing (migrated from custom_logic path)
			$chunk_size  = apply_filters( 'pneupex_import_chunk_size', 50 );
			$chunk_stats = $this->process_products_chunked( $valid_products, $chunk_size );

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

		$this->logger->info( "Import completed. Processed: {$stats['processed']}, Errors: {$stats['errors']}, Skipped: {$stats['skipped']}." );

		return $stats;
	}

	/**
	 * Download file via HTTP URL using WordPress HTTP API (with timeout)
	 * Replaces file_get_contents() to prevent cron hangs and improve error handling
	 */
	protected function download_file(): ?string {
		// Get URL from constant or use default
		$url = defined( 'PNEUPEX_LATEX_IMPORT_URL' ) ? PNEUPEX_LATEX_IMPORT_URL : 'https://xpartner.net.pl/index.php?a=b2b_api_offerFile&m=getFile&hash=c73deb927b8beb7e78719c792cc2406f';

		$this->logger->info( "Downloading file from URL: {$url}" );

		// Use WordPress HTTP API with timeout to prevent cron hangs
		$response = wp_remote_get( $url, array( 'timeout' => 60 ) );

		if ( is_wp_error( $response ) ) {
			$this->logger->error( 'Download failed: ' . $response->get_error_message() );
			return null;
		}

		$response_code = wp_remote_retrieve_response_code( $response );
		if ( $response_code !== 200 ) {
			$this->logger->error( "Download failed with HTTP status: {$response_code}" );
			return null;
		}

		$source = wp_remote_retrieve_body( $response );

		if ( empty( $source ) ) {
			$this->logger->error( 'Downloaded file is empty.' );
			return null;
		}

		// Save to temp directory
		$ftp_handler = new Pneupex_FTP_Handler();
		$temp_dir    = $ftp_handler->get_temp_dir();

		if ( ! $temp_dir ) {
			$this->logger->error( 'Failed to get temp directory.' );
			return null;
		}

		$local_file = $temp_dir . 'import-products-latex_' . time() . '.csv';

		if ( file_put_contents( $local_file, $source ) === false ) {
			$this->logger->error( 'Failed to save downloaded file.' );
			return null;
		}

		return $local_file;
	}

	/**
	 * Parse CSV file
	 * Original: Skip first row (header), then process remaining rows
	 * Original uses semicolon delimiter: fgetcsv($this->_file, 0, ';')
	 */
	protected function parse_csv( string $file_path ): array {
		$products  = array();
		$first_row = true;

		if ( ( $handle = fopen( $file_path, 'r' ) ) !== false ) {
			// Original uses semicolon delimiter
			$delimiter = ';';

			while ( ( $row = fgetcsv( $handle, 0, $delimiter, '"', '\\' ) ) !== false ) {
				// Skip first row (header) - original logic
				if ( $first_row ) {
					$first_row = false;
					continue;
				}

				// Only add rows that have at least EAN column
				if ( isset( $row[ self::COL_EAN ] ) ) {
					$products[] = $row;
				}
			}
			fclose( $handle );
		}

		return $products;
	}

	/**
	 * Check if row should be skipped based on original logic
	 * Original: Skip if EAN is empty or '0', or price is 0, or AGE is 'STARE DOTY'
	 */
	private function should_skip_row( array $row ): bool {
		$ean   = isset( $row[ self::COL_EAN ] ) ? trim( (string) $row[ self::COL_EAN ] ) : '';
		$price = isset( $row[ self::COL_PRICE ] ) ? (float) $row[ self::COL_PRICE ] : 0;
		$age   = isset( $row[ self::COL_AGE ] ) ? trim( (string) $row[ self::COL_AGE ] ) : '';

		// Skip if EAN is empty or '0'
		if ( empty( $ean ) || $ean === '0' ) {
			return true;
		}

		// Skip if price is 0
		if ( $price == 0 ) {
			return true;
		}

		// Skip if AGE is 'STARE DOTY'
		if ( $age === 'STARE DOTY' ) {
			return true;
		}

		return false;
	}

	protected function extract_sku( array $row ): string {
		return isset( $row[ self::COL_EAN ] ) ? trim( (string) $row[ self::COL_EAN ] ) : '';
	}

	protected function extract_quantity( array $row ): int {
		return isset( $row[ self::COL_ONSTOCK ] ) ? (int) $row[ self::COL_ONSTOCK ] : 0;
	}

	protected function extract_price( array $row ): float {
		return isset( $row[ self::COL_PRICE ] ) ? (float) $row[ self::COL_PRICE ] : 0.0;
	}

	/**
	 * Override to update additional meta fields after bulk warehouse update
	 * Handles Latex-specific logic: DOT and ISDEMO
	 */
	protected function update_additional_product_meta( array $warehouse_updates ): void {
		$meta_updates = array();
		foreach ( $warehouse_updates as $product_id => $data ) {
			$csv_row      = $data['csv_row'] ?? array();
			$product_meta = array();

			// Process DOT (original logic: if string, take substring from position 3)
			$dot = isset( $csv_row[ self::COL_DOT ] ) ? $csv_row[ self::COL_DOT ] : null;
			if ( ! empty( $dot ) && is_string( $dot ) ) {
				$dot_value = (int) substr( $dot, 3 );
				if ( $dot_value > 0 ) {
					$product_meta['dot'] = $dot_value;
				}
			}

			// Process ISDEMO (original logic: check if "DEMO DM")
			$isdemo = isset( $csv_row[ self::COL_ISDEMO ] ) ? trim( (string) $csv_row[ self::COL_ISDEMO ] ) : '';
			if ( $isdemo === 'DEMO DM' ) {
				$product_meta['is_demo'] = '1';
			}

			if ( ! empty( $product_meta ) ) {
				$meta_updates[ $product_id ] = $product_meta;
			}
		}

		if ( ! empty( $meta_updates ) ) {
			$this->bulk_update_post_meta( $meta_updates );
		}
	}
}
