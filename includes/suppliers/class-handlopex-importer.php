<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

require_once __DIR__ . '/../class-supplier-importer-base.php';
require_once __DIR__ . '/../class-ftp-handler.php';

/**
 * Handlopex supplier importer
 * Based on ext-stock-handlopex.php
 * Uses FTP URL to download 3 CSV files: pneupex_mw.csv, pneupex_mde.csv, pneupex_mx.csv
 * EAN in col[12], Price in col[10], Quantity in col[9], DOT in col[16]
 */
class Pneupex_Handlopex_Importer extends Pneupex_Supplier_Importer_Base {

	public function __construct() {
		parent::__construct( 'Handlopex' );
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
			// Process 3 files
			$files = array(
				'pneupex_mw.csv',
				'pneupex_mde.csv',
				'pneupex_mx.csv',
			);

			foreach ( $files as $file ) {
				$file_path = $this->download_single_file( $file );
				if ( ! $file_path ) {
					$this->logger->warning( "Failed to download file: {$file}" );
					continue;
				}

				$this->logger->info( "File downloaded: {$file_path}" );

				$products = $this->parse_csv( $file_path );
				$this->logger->info( 'Found ' . count( $products ) . " products in {$file}." );

				// Apply row limit for testing (if specified)
				$products = $this->apply_row_limit( $products, $row_limit );

				// Use optimized bulk processing (migrated from custom_logic path)
				$chunk_size  = apply_filters( 'pneupex_import_chunk_size', 50 ); // Default 50, can be filtered
				$chunk_stats = $this->process_products_chunked( $products, $chunk_size );

				// Merge chunk stats into main stats
				$stats['processed'] += $chunk_stats['processed'];
				$stats['updated']   += $chunk_stats['updated'];
				$stats['errors']    += $chunk_stats['errors'];
				$stats['skipped']   += $chunk_stats['skipped'];

				$ftp_handler = new Pneupex_FTP_Handler();
				$ftp_handler->cleanup( $file_path );
			}
		} catch ( \Exception $e ) {
			$this->logger->error( 'Import failed: ' . $e->getMessage() );
			++$stats['errors'];
		}

		$this->logger->info( "Import completed. Processed: {$stats['processed']}, Errors: {$stats['errors']}." );

		return $stats;
	}

	/**
	 * Download file via FTP URL
	 * Handlopex downloads 3 files, so we implement this as a stub that calls download_single_file
	 */
	protected function download_file(): ?string {
		// This method is required by base class but not used for Handlopex
		// We use download_single_file() instead in import() method
		return null;
	}

	/**
	 * Download a single file via FTP URL
	 * Uses common FTP handler method for consistency
	 */
	protected function download_single_file( string $filename ): ?string {
		$host     = defined( 'PNEUPEX_HANDLOPEX_FTP_HOST' ) ? PNEUPEX_HANDLOPEX_FTP_HOST : 'ftp.pneupex.sk';
		$username = defined( 'PNEUPEX_HANDLOPEX_FTP_USER' ) ? PNEUPEX_HANDLOPEX_FTP_USER : '';
		$password = defined( 'PNEUPEX_HANDLOPEX_FTP_PASS' ) ? PNEUPEX_HANDLOPEX_FTP_PASS : '';
		$base_dir = defined( 'PNEUPEX_HANDLOPEX_FTP_DIR' ) ? PNEUPEX_HANDLOPEX_FTP_DIR : 'handlopex';

		if ( empty( $username ) || empty( $password ) ) {
			$this->logger->error( 'FTP credentials not defined in constants.' );
			return null;
		}

		// Build FTP URL: ftp://username:password@host/path/filename
		$base_dir = trim( $base_dir, '/' );
		$filename = trim( $filename, '/' );
		$url      = sprintf(
			'ftp://%s:%s@%s/%s/%s',
			rawurlencode( $username ),
			rawurlencode( $password ),
			$host,
			$base_dir,
			$filename
		);

		$this->logger->info( "Downloading file via FTP URL: ftp://{$username}:***@{$host}/{$base_dir}/{$filename}" );

		$ftp_handler = new Pneupex_FTP_Handler();
		$local_file  = $ftp_handler->download_via_url( $url );

		if ( $local_file ) {
			$file_size = filesize( $local_file );
			$this->logger->info( "Successfully downloaded file: {$filename} ({$file_size} bytes)" );
		} else {
			$this->logger->error( "Failed to download file via FTP URL: {$filename}" );
		}

		return $local_file;
	}

	/**
	 * Parse CSV file
	 * Original: Skip first row, EAN in col[12], Price in col[10], Quantity in col[9]
	 */
	protected function parse_csv( string $file_path ): array {
		$products  = array();
		$first_row = true;

		if ( ( $handle = fopen( $file_path, 'r' ) ) !== false ) {
			while ( ( $row = fgetcsv( $handle, 1000, ';', '"', '\\' ) ) !== false ) {
				// Skip first row
				if ( $first_row ) {
					$first_row = false;
					continue;
				}

				// EAN in col[12]
				if ( ! empty( $row[12] ) ) {
					$products[] = $row;
				}
			}
			fclose( $handle );
		}

		return $products;
	}

	protected function extract_sku( array $row ): string {
		return isset( $row[12] ) ? trim( (string) $row[12] ) : '';
	}

	protected function extract_quantity( array $row ): int {
		// Quantity in col[9]
		return isset( $row[9] ) ? (int) $row[9] : 0;
	}

	protected function extract_price( array $row ): float {
		// Price in col[10]
		return isset( $row[10] ) ? (float) $row[10] : 0.0;
	}

	/**
	 * Override to update additional meta fields after bulk warehouse update
	 * Updates 'eprel' meta from CSV column 22
	 */
	protected function update_additional_product_meta( array $warehouse_updates ): void {
		$meta_updates = array();
		foreach ( $warehouse_updates as $product_id => $data ) {
			$csv_row = $data['csv_row'] ?? array();

			// Update eprel if available (col[22])
			if ( ! empty( $csv_row[22] ) && is_numeric( $csv_row[22] ) ) {
				$meta_updates[ $product_id ] = array( 'eprel' => $csv_row[22] );
			}
		}

		if ( ! empty( $meta_updates ) ) {
			$this->bulk_update_post_meta( $meta_updates );
		}
	}
}
