<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

require_once __DIR__ . '/../class-supplier-importer-base.php';
require_once __DIR__ . '/../class-ftp-handler.php';

/**
 * DobrePneu.cz supplier importer
 * Based on ext-stock-dobrepneu.php
 * Uses FTP URL to download CSV
 * Skip first row, EAN in col[1], Price in col[5] (with comma), Quantity in col[14]
 * Special: Price calculation adds 4.2 to final price
 */
class Pneupex_Dobrepneu_Importer extends Pneupex_Supplier_Importer_Base {

	public function __construct() {
		parent::__construct( 'DobrePneu.cz' );
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
	 * Original: ftp://mh040401:...@ftp.pneupex.sk/dobrepneu/dobrepneu-765069.csv
	 */
	protected function download_file(): ?string {
		$host        = defined( 'PNEUPEX_DOBREPNEU_FTP_HOST' ) ? PNEUPEX_DOBREPNEU_FTP_HOST : 'ftp.pneupex.sk';
		$username    = defined( 'PNEUPEX_DOBREPNEU_FTP_USER' ) ? PNEUPEX_DOBREPNEU_FTP_USER : '';
		$password    = defined( 'PNEUPEX_DOBREPNEU_FTP_PASS' ) ? PNEUPEX_DOBREPNEU_FTP_PASS : '';
		$remote_file = defined( 'PNEUPEX_DOBREPNEU_FTP_FILE' ) ? PNEUPEX_DOBREPNEU_FTP_FILE : 'dobrepneu/dobrepneu-765069.csv';

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
	 * Original: Skip first row, EAN in col[1]
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

				// EAN in col[1]
				if ( ! empty( $row[1] ) ) {
					$products[] = $row;
				}
			}
			fclose( $handle );
		}

		return $products;
	}

	protected function extract_sku( array $row ): string {
		return isset( $row[1] ) ? trim( (string) $row[1] ) : '';
	}

	protected function extract_quantity( array $row ): int {
		// Quantity in col[14]
		return ! empty( $row[14] ) ? (int) round( (float) $row[14] ) : 0;
	}

	protected function extract_price( array $row ): float {
		// Price in col[5], replace comma with dot
		if ( ! empty( $row[5] ) ) {
			return (float) round( (float) str_replace( ',', '.', $row[5] ), 2 );
		}
		return 0.0;
	}

	/**
	 * Override to add 4.2 to final price after coefficient calculation
	 */
	protected function calculate_price( float $base_price, int $product_id ): float {
		// Calculate price: (price from CSV * coefficient) + 4.2
		return parent::calculate_price( $base_price, $product_id ) + 4.2;
	}
}
