<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

require_once __DIR__ . '/../class-supplier-importer-base.php';
require_once __DIR__ . '/../class-ftp-handler.php';

/**
 * IHLE supplier importer
 * Based on ext-stock-ihle.php
 * Uses FTP URL (file_get_contents) to download CSV
 * Special: Has price in CSV (col[16]), quantity in col[9], EAN in col[4] or col[29]
 * Also updates eprel field from col[40]
 */
class Pneupex_IHLE_Importer extends Pneupex_Supplier_Importer_Base {

	public function __construct() {
		parent::__construct( 'IHLE' );
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
		// Note: Don't close log here - let admin handler close it after Modulario sync

		return $stats;
	}

	/**
	 * Download file via FTP URL
	 * Original: ftp://mh040401:6ZwG70eRXWJRtWZ9VgRnh9@ftp.pneupex.sk/ihle/PNEUPEXSKMug.csv
	 */
	protected function download_file(): ?string {
		$url = defined( 'PNEUPEX_IHLE_FTP_URL' ) ? PNEUPEX_IHLE_FTP_URL : '';

		if ( empty( $url ) ) {
			$host        = defined( 'PNEUPEX_IHLE_FTP_HOST' ) ? PNEUPEX_IHLE_FTP_HOST : 'ftp.pneupex.sk';
			$username    = defined( 'PNEUPEX_IHLE_FTP_USER' ) ? PNEUPEX_IHLE_FTP_USER : '';
			$password    = defined( 'PNEUPEX_IHLE_FTP_PASS' ) ? PNEUPEX_IHLE_FTP_PASS : '';
			$remote_file = defined( 'PNEUPEX_IHLE_FTP_FILE' ) ? PNEUPEX_IHLE_FTP_FILE : 'ihle/PNEUPEXSKMug.csv';

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
	 * Original: Skip first row, EAN in col[4] or col[29], Quantity in col[9], Price in col[16]
	 */
	protected function parse_csv( string $file_path ): array {
		$products = array();
		$index    = 0;

		if ( ( $handle = fopen( $file_path, 'r' ) ) !== false ) {
			while ( ( $row = fgetcsv( $handle, 0, ';', '"', '\\' ) ) !== false ) {
				// Skip first row
				if ( $index++ == 0 ) {
					continue;
				}

				// EAN in col[4] or col[29]
				$ean = ! empty( $row[4] ) ? $row[4] : ( ! empty( $row[29] ) ? $row[29] : '' );

				if ( ! empty( $ean ) ) {
					$products[] = $row;
				}
			}
			fclose( $handle );
		}

		return $products;
	}

	protected function extract_sku( array $row ): string {
		// EAN in col[4] or col[29]
		$ean = ! empty( $row[4] ) ? $row[4] : ( ! empty( $row[29] ) ? $row[29] : '' );
		return trim( (string) $ean );
	}

	protected function extract_quantity( array $row ): int {
		// Quantity in col[9]
		return ! empty( $row[9] ) ? (int) round( (float) $row[9] ) : 0;
	}

	protected function extract_price( array $row ): float {
		// Price in col[16]
		return ! empty( $row[16] ) ? (float) round( (float) $row[16], 2 ) : 0.0;
	}

	/**
	 * Override to add 2.00 to final price after coefficient calculation
	 */
	protected function calculate_price( float $base_price, int $product_id ): float {
		// Calculate price: (price from CSV * coefficient) + 2.00
		return parent::calculate_price( $base_price, $product_id ) + 2.00;
	}

	/**
	 * Override to update additional meta fields after bulk warehouse update
	 * Updates 'eprel' meta from CSV column 40
	 */
	protected function update_additional_product_meta( array $warehouse_updates ): void {
		$meta_updates = array();
		foreach ( $warehouse_updates as $product_id => $data ) {
			$csv_row = $data['csv_row'] ?? array();

			// Update eprel field if available (col[40])
			if ( ! empty( $csv_row[40] ) ) {
				$eprel = $csv_row[40];

				// Extract number from URL if it's a URL
				if ( strpos( $eprel, '/' ) !== false ) {
					$pos = strrpos( $eprel, '/' );
					if ( $pos !== false ) {
						$eprel = substr( $eprel, $pos + 1 );
					}
				}

				if ( is_numeric( $eprel ) ) {
					$meta_updates[ $product_id ] = array( 'eprel' => $eprel );
					// $this->logger->debug( "Updated eprel for product {$product_id}: {$eprel}" );
				}
			}
		}

		if ( ! empty( $meta_updates ) ) {
			$this->bulk_update_post_meta( $meta_updates );
		}
	}
}
