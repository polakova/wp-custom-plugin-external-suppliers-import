<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

require_once __DIR__ . '/../class-supplier-importer-base.php';
require_once __DIR__ . '/../class-ftp-handler.php';

/**
 * Asteel supplier importer
 * Based on ext-stock-asteel.php
 * Uses FTP URL with date in filename: stock_asteel_data_YYYY-MM-DD.csv
 * EAN in col[15], Price in col[17], Quantity in col[16], DOT in col[18]
 */
class Pneupex_Asteel_Importer extends Pneupex_Supplier_Importer_Base {

	public function __construct() {
		parent::__construct( 'Asteel' );
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
	 * Original: ftp://tyrestock_asteel_export_stock:...@server.tyrestock.sk/stock_asteel_data_YYYY-MM-DD.csv
	 */
	protected function download_file(): ?string {
		$host         = defined( 'PNEUPEX_ASTEEL_FTP_HOST' ) ? PNEUPEX_ASTEEL_FTP_HOST : 'server.tyrestock.sk';
		$username     = defined( 'PNEUPEX_ASTEEL_FTP_USER' ) ? PNEUPEX_ASTEEL_FTP_USER : '';
		$password     = defined( 'PNEUPEX_ASTEEL_FTP_PASS' ) ? PNEUPEX_ASTEEL_FTP_PASS : '';
		$current_date = date( 'Y-m-d' );
		$remote_file  = defined( 'PNEUPEX_ASTEEL_FTP_FILE_PATTERN' )
			? str_replace( '{date}', $current_date, PNEUPEX_ASTEEL_FTP_FILE_PATTERN )
			: "stock_asteel_data_{$current_date}.csv";

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
	 * Original: EAN in col[15], Price in col[17], Quantity in col[16]
	 */
	protected function parse_csv( string $file_path ): array {
		$products = array();

		if ( ( $handle = fopen( $file_path, 'r' ) ) !== false ) {
			while ( ( $row = fgetcsv( $handle, 1000, ';', '"', '\\' ) ) !== false ) {
				// EAN in col[15]
				if ( ! empty( $row[15] ) ) {
					$products[] = $row;
				}
			}
			fclose( $handle );
		}

		return $products;
	}

	protected function extract_sku( array $row ): string {
		return isset( $row[15] ) ? trim( (string) $row[15] ) : '';
	}

	protected function extract_quantity( array $row ): int {
		// Quantity in col[16]
		return isset( $row[16] ) ? (int) $row[16] : 0;
	}

	protected function extract_price( array $row ): float {
		// Price in col[17]
		return isset( $row[17] ) ? (float) $row[17] : 0.0;
	}
}
