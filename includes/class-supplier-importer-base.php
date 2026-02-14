<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

/**
 * Base class for all supplier importers.
 * Handles common functionality: product matching, ACF updates, coefficient calculation.
 */
abstract class Pneupex_Supplier_Importer_Base {

	/**
	 * Supplier name (e.g., 'Michelin', 'Continental')
	 */
	protected string $supplier_name;

	/**
	 * Supplier ID (number 1-x from mapper)
	 */
	protected int $supplier_id;

	/**
	 * Logger instance
	 */
	protected Pneupex_Supplier_Logger $logger;

	/**
	 * Coefficient manager instance
	 */
	protected Pneupex_Coefficient_Manager $coefficient_manager;

	/**
	 * Close log file (public method for external closing after Modulario sync)
	 */
	public function close_log(): void {
		$this->logger->close_log();
	}

	/**
	 * Batch size for Modulario sync (products per batch)
	 * Can be overridden via filter: apply_filters('pneupex_modulario_sync_batch_size', 100)
	 */
	private const MODULARIO_SYNC_BATCH_SIZE = 100;

	/**
	 * Rate limiting: minimum delay between API calls in microseconds
	 * Default: 2 seconds (2000000 microseconds)
	 * Can be overridden via filter: apply_filters('pneupex_modulario_rate_limit_delay', 2000000)
	 */
	private const MODULARIO_RATE_LIMIT_DELAY = 2000000;

	/**
	 * Rate limiting: maximum requests per minute
	 * Default: 30 requests per minute
	 * Can be overridden via filter: apply_filters('pneupex_modulario_rate_limit_per_minute', 30)
	 */
	private const MODULARIO_RATE_LIMIT_PER_MINUTE = 30;

	/**
	 * Track API call timestamps for rate limiting
	 *
	 * @var array Array of timestamps of recent API calls
	 */
	private static array $api_call_timestamps = array();

	/**
	 * Apply rate limiting before API call
	 * Enforces minimum delay between calls and maximum requests per minute
	 */
	private function apply_rate_limiting(): void {
		$delay_microseconds = (int) apply_filters( 'pneupex_modulario_rate_limit_delay', self::MODULARIO_RATE_LIMIT_DELAY );
		$max_per_minute     = (int) apply_filters( 'pneupex_modulario_rate_limit_per_minute', self::MODULARIO_RATE_LIMIT_PER_MINUTE );

		$now = microtime( true );

		// Clean old timestamps (older than 1 minute)
		self::$api_call_timestamps = array_filter(
			self::$api_call_timestamps,
			function ( $timestamp ) use ( $now ) {
				return ( $now - $timestamp ) < 60;
			}
		);

		// Check if we've exceeded rate limit
		if ( count( self::$api_call_timestamps ) >= $max_per_minute ) {
			$oldest_call  = min( self::$api_call_timestamps );
			$wait_seconds = 60 - ( $now - $oldest_call );

			if ( $wait_seconds > 0 ) {
				$this->logger->warning(
					sprintf(
						'Rate limit reached (%d requests/minute). Waiting %.2f seconds before next API call.',
						$max_per_minute,
						$wait_seconds
					)
				);
				sleep( (int) ceil( $wait_seconds ) );
			}
		}

		// Apply minimum delay since last call
		if ( ! empty( self::$api_call_timestamps ) ) {
			$last_call       = max( self::$api_call_timestamps );
			$time_since_last = ( $now - $last_call ) * 1000000; // Convert to microseconds

			if ( $time_since_last < $delay_microseconds ) {
				$wait_microseconds = $delay_microseconds - $time_since_last;
				usleep( (int) $wait_microseconds );
			}
		}

		// Record this API call
		self::$api_call_timestamps[] = microtime( true );
	}

	/**
	 * Batch size for Modulario sync
	 * Can be overridden via filter: apply_filters('pneupex_modulario_sync_batch_size', 200)
	 */

	/**
	 * Track product IDs that were updated during import
	 */
	private array $updated_product_ids = array();

	/**
	 * Get updated product IDs (for cross-supplier deduplication)
	 *
	 * @return array Product IDs that were updated during this import
	 */
	public function get_updated_product_ids(): array {
		return $this->updated_product_ids;
	}

	/**
	 * Log counter for reduced logging during bulk import
	 */
	private int $log_counter = 0;

	/**
	 * Start time for time limit tracking (reset at start of each import)
	 */
	private ?float $import_start_time = null;

	/**
	 * ACF field keys (from wc-warehouse.php)
	 * When reading raw data, use field keys. When updating, ACF accepts field names.
	 */
	protected const ACF_KEY_ID           = 'field_69397eb694381';
	protected const ACF_KEY_QTY          = 'field_69397ecb94382';
	protected const ACF_KEY_PRICE        = 'field_69397ee694383';
	protected const ACF_FIELD_NAME_PRICE = 'external_warehouse_product_price';
	protected const ACF_FIELD_NAME_ID    = 'external_warehouse_id';
	protected const ACF_FIELD_NAME_QTY   = 'external_warehouse_product_quantity';

	/**
	 * Constructor
	 */
	public function __construct( string $supplier_name ) {
		$this->supplier_name       = $supplier_name;
		$this->logger              = new Pneupex_Supplier_Logger( $supplier_name );
		$this->coefficient_manager = new Pneupex_Coefficient_Manager();

		// Get supplier ID from mapper
		$mapper            = new Pneupex_Supplier_ID_Mapper();
		$this->supplier_id = $mapper->get_supplier_id( $supplier_name );

		if ( $this->supplier_id === 0 ) {
			$this->logger->error( "Supplier '{$supplier_name}' not found in ID mapper. Using default ID 0." );
		}
	}

	/**
	 * Main import method - must be implemented by each supplier
	 *
	 * @param int|null $row_limit Optional: Limit number of CSV rows to process (for testing). Null = process all rows.
	 * @return array Statistics: ['processed' => int, 'updated' => int, 'errors' => int, 'skipped' => int]
	 */
	abstract public function import( ?int $row_limit = null ): array;

	/**
	 * Helper method to apply row limit to CSV data
	 * Call this after parse_csv() to limit rows for testing
	 *
	 * @param array    $csv_rows Parsed CSV rows
	 * @param int|null $row_limit Row limit (null = no limit)
	 * @return array Limited CSV rows
	 */
	protected function apply_row_limit( array $csv_rows, ?int $row_limit ): array {
		if ( $row_limit === null || $row_limit <= 0 ) {
			return $csv_rows;
		}

		$total_found = count( $csv_rows );
		$limited     = array_slice( $csv_rows, 0, $row_limit );

		$this->logger->info(
			sprintf(
				'Row limit applied: Processing only first %d rows (out of %d total).',
				$row_limit,
				$total_found
			)
		);

		return $limited;
	}

	/**
	 * Download CSV file from supplier
	 * Must be implemented by each supplier (FTP/SFTP varies)
	 */
	abstract protected function download_file(): ?string;

	/**
	 * Parse CSV and return array of product data
	 * Must be implemented by each supplier (column positions vary)
	 */
	abstract protected function parse_csv( string $file_path ): array;

	/**
	 * Get product by SKU (EAN from CSV = SKU in WooCommerce)
	 * For batch processing, use get_products_by_skus() instead
	 */
	protected function get_product_by_sku( string $sku ): ?int {
		if ( ! function_exists( 'wc_get_product_id_by_sku' ) ) {
			$this->logger->error( 'WooCommerce function wc_get_product_id_by_sku not available.' );
			return null;
		}

		$product_id = wc_get_product_id_by_sku( $sku );

		if ( ! $product_id ) {
			$this->logger->debug( "Product with SKU '{$sku}' not found." );
			return null;
		}

		return $product_id;
	}

	/**
	 * Batch lookup products by SKU (optimized for large imports)
	 * Returns array: ['sku' => product_id, ...]
	 */
	protected function get_products_by_skus( array $skus ): array {
		global $wpdb;

		if ( empty( $skus ) ) {
			return array();
		}

		// Remove empty SKUs and sanitize
		$skus = array_filter( array_map( 'trim', $skus ) );
		if ( empty( $skus ) ) {
			return array();
		}

		// Prepare placeholders for IN clause
		$placeholders = implode( ',', array_fill( 0, count( $skus ), '%s' ) );

		// Query postmeta for SKUs (WooCommerce stores SKU in _sku meta key)
		$query = $wpdb->prepare(
			"SELECT pm.meta_value as sku, pm.post_id as product_id
             FROM {$wpdb->postmeta} pm
             INNER JOIN {$wpdb->posts} p ON pm.post_id = p.ID
             WHERE pm.meta_key = '_sku'
             AND pm.meta_value IN ({$placeholders})
             AND p.post_type = 'product'
             AND p.post_status != 'trash'",
			...$skus
		);

		$results = $wpdb->get_results( $query, ARRAY_A );

		$mapping = array();
		foreach ( $results as $row ) {
			$mapping[ $row['sku'] ] = (int) $row['product_id'];
		}

		return $mapping;
	}

	/**
	 * Batch load product terms for all products in chunk (optimized)
	 * Pre-loads term relationships to avoid N+1 queries
	 *
	 * @param array $product_ids Array of product IDs
	 */
	protected function batch_load_product_terms( array $product_ids ): void {
		if ( empty( $product_ids ) ) {
			return;
		}

		// Batch load term relationships for both taxonomies
		// This pre-populates the cache so wp_get_post_terms() is fast
		wp_get_object_terms(
			$product_ids,
			array( 'pa_typ', 'pa_vyrobca' ),
			array(
				'fields'                 => 'all',
				'update_term_meta_cache' => false, // Don't load term meta, we only need names
			)
		);
	}

	/**
	 * Get product type and brand for coefficient calculation
	 * Optimized to use direct term queries instead of loading full product object
	 * Terms should be pre-loaded via batch_load_product_terms() for best performance
	 */
	protected function get_product_attributes( int $product_id ): array {
		// Use direct term queries instead of loading full product object (faster)
		// Terms should already be cached from batch_load_product_terms()
		// Get product type (attribute id 1 in original = 'typ' taxonomy)
		$type_terms = wp_get_post_terms( $product_id, 'pa_typ', array( 'fields' => 'names' ) );
		$type       = ! is_wp_error( $type_terms ) && ! empty( $type_terms ) ? $type_terms[0] : '';

		// Get brand (attribute id 2 in original = 'vyrobca' taxonomy)
		$brand_terms = wp_get_post_terms( $product_id, 'pa_vyrobca', array( 'fields' => 'names' ) );
		$brand       = ! is_wp_error( $brand_terms ) && ! empty( $brand_terms ) ? $brand_terms[0] : '';

		return array(
			'type'  => $type,
			'brand' => $brand,
		);
	}

	/**
	 * Calculate price using coefficient
	 */
	protected function calculate_price( float $base_price, int $product_id ): float {
		$attributes  = $this->get_product_attributes( $product_id );
		$coefficient = $this->coefficient_manager->get_coefficient(
			$this->supplier_name,
			$attributes['type'],
			$attributes['brand']
		);

		return round( $base_price * $coefficient, 2 );
	}

	/**
	 * Get base price from product (for suppliers that don't have price in CSV)
	 * Override this method in supplier class to get price from product instead of CSV
	 *
	 * @param int   $product_id Product ID
	 * @param array $csv_row CSV row data (for reference)
	 * @return float Base price from product, or 0.0 if not available
	 */
	protected function get_base_price_from_product( int $product_id, array $csv_row ): float {
		// Default: return 0.0 (use CSV price)
		// Suppliers like Goodyear2 can override this to return $product->get_price()
		return 0.0;
	}

	/**
	 * Update additional product meta fields after bulk warehouse update
	 * Override this method in supplier class to update custom meta fields
	 * (e.g., Handlopex updates 'eprel' meta from CSV column 22)
	 *
	 * @param array $warehouse_updates Array of [product_id => ['quantity' => int, 'price' => float, 'csv_row' => array], ...]
	 */
	protected function update_additional_product_meta( array $warehouse_updates ): void {
		// Default: no additional meta updates
		// Suppliers can override this to update custom fields
		// Example (Handlopex):
		// foreach ( $warehouse_updates as $product_id => $data ) {
		// $csv_row = $data['csv_row'] ?? [];
		// if ( ! empty( $csv_row[22] ) && is_numeric( $csv_row[22] ) ) {
		// update_post_meta( $product_id, 'eprel', $csv_row[22] );
		// }
		// }
	}

	/**
	 * Bulk update stock status for multiple products
	 * Efficiently replaces WC_ACF_Sync_Handler::execute_logic() for bulk operations
	 *
	 * @param array      $product_ids Array of product IDs to update
	 * @param array|null $preloaded_warehouse_data Optional: Pre-loaded warehouse data [product_id => raw_rows_array, ...]
	 *                                             If provided, avoids re-reading from DB
	 * @return int Number of products updated
	 */
	protected function bulk_update_stock_status( array $product_ids, ?array $preloaded_warehouse_data = null ): int {
		if ( empty( $product_ids ) ) {
			return 0;
		}

		// Batch load all local stock quantities
		$local_stocks = $this->batch_load_local_stock( $product_ids );

		// Use pre-loaded warehouse data if available, otherwise load from DB
		if ( $preloaded_warehouse_data !== null ) {
			// Calculate best external options from pre-loaded data (avoids DB query)
			$external_data = array();
			foreach ( $product_ids as $product_id ) {
				$raw_rows     = $preloaded_warehouse_data[ $product_id ] ?? array();
				$temp_options = array();

				foreach ( $raw_rows as $row ) {
					$q = isset( $row[ self::ACF_KEY_QTY ] ) ? (int) $row[ self::ACF_KEY_QTY ] : 0;
					$p = isset( $row[ self::ACF_KEY_PRICE ] ) ? (float) $row[ self::ACF_KEY_PRICE ] : 0.0;

					// Only include options with qty >= 4 and price > 0
					if ( $q >= 4 && $p > 0 ) {
						$temp_options[] = array(
							'qty'   => $q,
							'price' => $p,
						);
					}
				}

				if ( ! empty( $temp_options ) ) {
					// Sort by price (cheapest first)
					usort(
						$temp_options,
						function ( $a, $b ) {
							return $a['price'] <=> $b['price'];
						}
					);
					$external_data[ $product_id ] = $temp_options[0];
				} else {
					$external_data[ $product_id ] = null;
				}
			}
		} else {
			// Fallback: Load from DB if not provided
			$external_data = $this->batch_get_best_external_options( $product_ids );
		}

		$updated_count = 0;
		$stock_updates = array();
		$meta_updates  = array();

		// Process each product
		foreach ( $product_ids as $product_id ) {
			$local_qty = $local_stocks[ $product_id ] ?? 0;
			$ext_data  = $external_data[ $product_id ] ?? null;
			$ext_qty   = ( $ext_data && isset( $ext_data['qty'] ) ) ? (int) $ext_data['qty'] : 0;

			// Cache external qty to meta
			$meta_updates[ $product_id ] = array(
				'_pneupex_cached_ext_qty' => $ext_qty,
			);

			// Determine stock status (same logic as WC_ACF_Sync_Handler::execute_logic)
			// Scenario A: Has Local Stock -> Always INSTOCK
			if ( $local_qty > 0 ) {
				$stock_updates[ $product_id ] = array(
					'stock_status' => 'instock',
					'manage_stock' => true,
				);
			}
			// Scenario B: No Local, but Has External -> ONBACKORDER
			elseif ( $ext_qty > 0 ) {
				$stock_updates[ $product_id ] = array(
					'stock_status' => 'onbackorder',
					'manage_stock' => false,
				);
			}
			// Scenario C: No Local, No External -> OUTOFSTOCK
			else {
				$stock_updates[ $product_id ] = array(
					'stock_status' => 'outofstock',
					'manage_stock' => true,
				);
			}
		}

		// Bulk update meta (external qty cache)
		if ( ! empty( $meta_updates ) ) {
			$this->bulk_update_post_meta( $meta_updates );
		}

		// Bulk update stock status and manage_stock flag
		if ( ! empty( $stock_updates ) ) {
			$this->bulk_update_stock_status_direct( $stock_updates );

			// Log summary of stock status updates with product IDs grouped by status
			$status_counts      = array(
				'instock'     => 0,
				'onbackorder' => 0,
				'outofstock'  => 0,
			);
			$status_product_ids = array(
				'instock'     => array(),
				'onbackorder' => array(),
				'outofstock'  => array(),
			);

			foreach ( $stock_updates as $product_id => $update ) {
				$status = $update['stock_status'] ?? '';
				if ( isset( $status_counts[ $status ] ) ) {
					++$status_counts[ $status ];
					$status_product_ids[ $status ][] = $product_id;
				}
			}

			// Build product IDs strings for each status
			$instock_ids     = ! empty( $status_product_ids['instock'] ) ? implode( ', ', $status_product_ids['instock'] ) : 'none';
			$onbackorder_ids = ! empty( $status_product_ids['onbackorder'] ) ? implode( ', ', $status_product_ids['onbackorder'] ) : 'none';
			$outofstock_ids  = ! empty( $status_product_ids['outofstock'] ) ? implode( ', ', $status_product_ids['outofstock'] ) : 'none';

			$this->logger->debug(
				sprintf(
					'Stock status updated for %d products: %d instock (%s), %d onbackorder (%s), %d outofstock (%s)',
					count( $stock_updates ),
					$status_counts['instock'],
					$instock_ids,
					$status_counts['onbackorder'],
					$onbackorder_ids,
					$status_counts['outofstock'],
					$outofstock_ids
				)
			);
		} else {
			$this->logger->debug( 'No stock status updates needed for this chunk' );
		}

		return count( $stock_updates );
	}

	/**
	 * Batch load local stock quantities for multiple products
	 *
	 * @param array $product_ids Array of product IDs
	 * @return array Array of [product_id => stock_quantity, ...]
	 */
	protected function batch_load_local_stock( array $product_ids ): array {
		global $wpdb;

		if ( empty( $product_ids ) ) {
			return array();
		}

		$placeholders = implode( ',', array_fill( 0, count( $product_ids ), '%d' ) );

		// Load stock quantities from postmeta
		$query = $wpdb->prepare(
			"SELECT post_id, meta_value 
             FROM {$wpdb->postmeta} 
             WHERE post_id IN ($placeholders) 
             AND meta_key = '_stock'",
			...$product_ids
		);

		$results = $wpdb->get_results( $query, ARRAY_A );

		$stocks = array();
		foreach ( $results as $row ) {
			$stocks[ $row['post_id'] ] = is_numeric( $row['meta_value'] ) ? (int) $row['meta_value'] : 0;
		}

		// Fill in 0 for products without stock meta
		foreach ( $product_ids as $product_id ) {
			if ( ! isset( $stocks[ $product_id ] ) ) {
				$stocks[ $product_id ] = 0;
			}
		}

		return $stocks;
	}

	/**
	 * Batch load ACF warehouse data for multiple products
	 * Uses direct meta queries for better performance
	 *
	 * @param array $product_ids Array of product IDs
	 * @return array Array of [product_id => raw_rows_array, ...]
	 */
	protected function batch_load_acf_warehouse_data( array $product_ids ): array {
		global $wpdb;

		if ( empty( $product_ids ) ) {
			return array();
		}

		$results      = array();
		$placeholders = implode( ',', array_fill( 0, count( $product_ids ), '%d' ) );

		// Load all repeater counts at once
		$counts_query    = $wpdb->prepare(
			"SELECT post_id, meta_value 
             FROM {$wpdb->postmeta} 
             WHERE post_id IN ($placeholders) 
             AND meta_key = 'external_warehouse'",
			...$product_ids
		);
		$counts          = $wpdb->get_results( $counts_query, ARRAY_A );
		$repeater_counts = array();
		foreach ( $counts as $row ) {
			$repeater_counts[ $row['post_id'] ] = is_numeric( $row['meta_value'] ) ? (int) $row['meta_value'] : 0;
		}

		// Load all repeater row data at once
		$meta_query = $wpdb->prepare(
			"SELECT post_id, meta_key, meta_value 
             FROM {$wpdb->postmeta} 
             WHERE post_id IN ($placeholders) 
             AND meta_key LIKE 'external_warehouse_%%'",
			...$product_ids
		);
		$all_meta   = $wpdb->get_results( $meta_query, ARRAY_A );

		// Organize meta by product and row index
		$product_meta = array();
		foreach ( $all_meta as $meta ) {
			$product_id = $meta['post_id'];
			if ( ! isset( $product_meta[ $product_id ] ) ) {
				$product_meta[ $product_id ] = array();
			}
			$product_meta[ $product_id ][] = $meta;
		}

		// Reconstruct ACF repeater structure for each product
		foreach ( $product_ids as $product_id ) {
			$count    = $repeater_counts[ $product_id ] ?? 0;
			$raw_rows = array();

			if ( $count > 0 && isset( $product_meta[ $product_id ] ) ) {
				// Parse meta keys to extract row index and field name
				$row_data = array();
				foreach ( $product_meta[ $product_id ] as $meta ) {
					// Parse meta key: external_warehouse_{$i}_external_warehouse_id
					// Use explode instead of regex for better performance (45,000+ calls)
					$key_parts = explode( '_', $meta['meta_key'], 4 );
					if ( count( $key_parts ) >= 4 && $key_parts[0] === 'external' && $key_parts[1] === 'warehouse' && is_numeric( $key_parts[2] ) ) {
						$row_index  = (int) $key_parts[2];
						$field_name = $key_parts[3];

						if ( ! isset( $row_data[ $row_index ] ) ) {
							$row_data[ $row_index ] = array();
						}

						// Map field names to ACF keys
						if ( $field_name === 'external_warehouse_id' ) {
							$row_data[ $row_index ][ self::ACF_KEY_ID ] = $meta['meta_value'];
						} elseif ( $field_name === 'external_warehouse_product_quantity' ) {
							$row_data[ $row_index ][ self::ACF_KEY_QTY ] = (int) $meta['meta_value'];
						} elseif ( $field_name === 'external_warehouse_product_price' ) {
							$row_data[ $row_index ][ self::ACF_KEY_PRICE ] = (float) $meta['meta_value'];
						}
					}
				}

				// Convert indexed array to sequential array
				ksort( $row_data );
				$raw_rows = array_values( $row_data );
			}

			$results[ $product_id ] = $raw_rows;
		}

		return $results;
	}

	/**
	 * Batch get best external options for multiple products
	 * Efficient version of pneupex_get_best_external_option() for bulk operations
	 *
	 * @param array $product_ids Array of product IDs
	 * @return array Array of [product_id => ['qty' => int, 'price' => float, 'id' => string], ...]
	 */
	protected function batch_get_best_external_options( array $product_ids ): array {
		if ( empty( $product_ids ) ) {
			return array();
		}

		// We already have the ACF data loaded, so we can use it directly
		// Load all external warehouse data in one query
		$all_warehouse_data = $this->batch_load_acf_warehouse_data( $product_ids );

		$results = array();

		foreach ( $product_ids as $product_id ) {
			$ext_rows = $all_warehouse_data[ $product_id ] ?? array();

			if ( empty( $ext_rows ) ) {
				$results[ $product_id ] = null;
				continue;
			}

			// Find best option (same logic as pneupex_get_best_external_option)
			$temp_options = array();
			foreach ( $ext_rows as $row ) {
				$q  = isset( $row[ self::ACF_KEY_QTY ] ) ? (int) $row[ self::ACF_KEY_QTY ] :
					( isset( $row[ self::ACF_FIELD_NAME_QTY ] ) ? (int) $row[ self::ACF_FIELD_NAME_QTY ] : 0 );
				$p  = isset( $row[ self::ACF_KEY_PRICE ] ) ? (float) $row[ self::ACF_KEY_PRICE ] :
					( isset( $row[ self::ACF_FIELD_NAME_PRICE ] ) ? (float) $row[ self::ACF_FIELD_NAME_PRICE ] : 0.0 );
				$id = isset( $row[ self::ACF_KEY_ID ] ) ? $row[ self::ACF_KEY_ID ] :
					( isset( $row[ self::ACF_FIELD_NAME_ID ] ) ? $row[ self::ACF_FIELD_NAME_ID ] : '' );

				// Only include options with qty >= 4 and price > 0
				if ( $q >= 4 && $p > 0 ) {
					$temp_options[] = array(
						'qty'   => $q,
						'price' => $p,
						'id'    => $id,
					);
				}
			}

			if ( ! empty( $temp_options ) ) {
				// Sort by price (cheapest first)
				usort(
					$temp_options,
					function ( $a, $b ) {
						return $a['price'] <=> $b['price'];
					}
				);
				$results[ $product_id ] = $temp_options[0];
			} else {
				$results[ $product_id ] = null;
			}
		}

		return $results;
	}

	/**
	 * Check if database supports transactions
	 *
	 * @return bool True if transactions are supported
	 */
	private function supports_transactions(): bool {
		global $wpdb;

		// Cache the result to avoid repeated queries
		static $cached_result = null;
		if ( $cached_result !== null ) {
			return $cached_result;
		}

		// Check if InnoDB engine is being used (supports transactions)
		// Use $wpdb->prefix to get the correct table name
		$table_name = $wpdb->postmeta;
		$db_name    = $wpdb->dbname;

		// Query information_schema to check table engine
		$engine = $wpdb->get_var(
			$wpdb->prepare(
				'SELECT ENGINE 
             FROM information_schema.TABLES 
             WHERE TABLE_SCHEMA = %s 
             AND TABLE_NAME = %s',
				$db_name,
				$table_name
			)
		);

		// InnoDB and XtraDB support transactions
		$cached_result = in_array( strtolower( $engine ?? '' ), array( 'innodb', 'xtradb' ), true );

		return $cached_result;
	}

	/**
	 * Bulk update post meta for multiple products
	 * FIXED: WordPress postmeta table doesn't have unique index on (post_id, meta_key),
	 * so we use DELETE + INSERT in a transaction to prevent duplicates
	 *
	 * @param array $updates Array of [product_id => ['meta_key' => 'meta_value', ...], ...]
	 */
	protected function bulk_update_post_meta( array $updates ): void {
		global $wpdb;

		if ( empty( $updates ) ) {
			return;
		}

		$use_transactions = $this->supports_transactions();

		// Start transaction for atomicity (if supported)
		if ( $use_transactions ) {
			$wpdb->query( 'START TRANSACTION' );
		}

		try {
			// First, delete existing meta records to prevent duplicates
			// WordPress allows multiple meta rows with same key, so we delete all matching ones
			foreach ( $updates as $product_id => $meta_data ) {
				foreach ( $meta_data as $meta_key => $meta_value ) {
					$wpdb->delete(
						$wpdb->postmeta,
						array(
							'post_id'  => $product_id,
							'meta_key' => $meta_key,
						),
						array( '%d', '%s' )
					);
				}
			}

			// Then insert new values
			$values       = array();
			$placeholders = array();

			foreach ( $updates as $product_id => $meta_data ) {
				foreach ( $meta_data as $meta_key => $meta_value ) {
					$values[]       = $product_id;
					$values[]       = $meta_key;
					$values[]       = maybe_serialize( $meta_value );
					$placeholders[] = '(%d, %s, %s)';
				}
			}

			if ( ! empty( $values ) ) {
				$sql = "INSERT INTO {$wpdb->postmeta} (post_id, meta_key, meta_value) VALUES " .
						implode( ', ', $placeholders );

				$wpdb->query( $wpdb->prepare( $sql, ...$values ) );
			}

			// Commit transaction (if supported)
			if ( $use_transactions ) {
				$wpdb->query( 'COMMIT' );
			}
		} catch ( \Exception $e ) {
			// Rollback on error (if transaction was started)
			if ( $use_transactions ) {
				$wpdb->query( 'ROLLBACK' );
			}
			$this->logger->error( 'Error in bulk_update_post_meta: ' . $e->getMessage() );
			throw $e;
		}
	}

	/**
	 * Bulk update stock status and manage_stock flag directly in database
	 * Bypasses WooCommerce product object and hooks for maximum performance
	 * Implements the same logic as WC_ACF_Sync_Handler::execute_logic() but in bulk
	 *
	 * @param array $updates Array of [product_id => ['stock_status' => string, 'manage_stock' => bool], ...]
	 */
	protected function bulk_update_stock_status_direct( array $updates ): void {
		global $wpdb;

		if ( empty( $updates ) ) {
			return;
		}

		// Temporarily remove hooks to prevent infinite loops
		$wc_save_removed   = false;
		$modulario_removed = false;

		if ( has_action( 'woocommerce_after_product_object_save', array( 'WC_ACF_Sync_Handler', 'handle_wc_save' ) ) ) {
			remove_action( 'woocommerce_after_product_object_save', array( 'WC_ACF_Sync_Handler', 'handle_wc_save' ), 10 );
			$wc_save_removed = true;
		}
		if ( has_action( 'woocommerce_update_product', 'update_product_in_modulario' ) ) {
			remove_action( 'woocommerce_update_product', 'update_product_in_modulario', 10 );
			$modulario_removed = true;
		}

		try {
			// Prepare bulk SQL updates for better performance (direct database updates)
			$stock_status_values       = array();
			$manage_stock_values       = array();
			$stock_status_placeholders = array();
			$manage_stock_placeholders = array();

			foreach ( $updates as $product_id => $data ) {
				// Prepare stock status update
				$stock_status_values[]       = $product_id;
				$stock_status_values[]       = '_stock_status';
				$stock_status_values[]       = $data['stock_status'];
				$stock_status_placeholders[] = '(%d, %s, %s)';

				// Prepare manage_stock update
				$manage_stock_values[]       = $product_id;
				$manage_stock_values[]       = '_manage_stock';
				$manage_stock_values[]       = $data['manage_stock'] ? 'yes' : 'no';
				$manage_stock_placeholders[] = '(%d, %s, %s)';
			}

			// Bulk update stock status (DELETE + INSERT to prevent duplicates)
			// WordPress postmeta doesn't have unique index on (post_id, meta_key)
			if ( ! empty( $stock_status_values ) ) {
				// Extract product IDs from values array (every 3rd element starting at index 0)
				$product_ids_for_stock = array();
				for ( $i = 0; $i < count( $stock_status_values ); $i += 3 ) {
					$product_ids_for_stock[] = $stock_status_values[ $i ];
				}
				$product_ids_for_stock = array_unique( $product_ids_for_stock );

				if ( ! empty( $product_ids_for_stock ) ) {
					$placeholders = implode( ',', array_fill( 0, count( $product_ids_for_stock ), '%d' ) );
					$wpdb->query(
						$wpdb->prepare(
							"DELETE FROM {$wpdb->postmeta} WHERE post_id IN ($placeholders) AND meta_key = '_stock_status'",
							...$product_ids_for_stock
						)
					);
				}

				// Insert new values
				$stock_status_sql = "INSERT INTO {$wpdb->postmeta} (post_id, meta_key, meta_value) VALUES " .
					implode( ', ', $stock_status_placeholders );

				$wpdb->query( $wpdb->prepare( $stock_status_sql, ...$stock_status_values ) );
			}

			// Bulk update manage_stock (DELETE + INSERT to prevent duplicates)
			if ( ! empty( $manage_stock_values ) ) {
				// Extract product IDs from values array (every 3rd element starting at index 0)
				$product_ids_for_manage = array();
				for ( $i = 0; $i < count( $manage_stock_values ); $i += 3 ) {
					$product_ids_for_manage[] = $manage_stock_values[ $i ];
				}
				$product_ids_for_manage = array_unique( $product_ids_for_manage );

				if ( ! empty( $product_ids_for_manage ) ) {
					$placeholders = implode( ',', array_fill( 0, count( $product_ids_for_manage ), '%d' ) );
					$wpdb->query(
						$wpdb->prepare(
							"DELETE FROM {$wpdb->postmeta} WHERE post_id IN ($placeholders) AND meta_key = '_manage_stock'",
							...$product_ids_for_manage
						)
					);
				}

				// Insert new values
				$manage_stock_sql = "INSERT INTO {$wpdb->postmeta} (post_id, meta_key, meta_value) VALUES " .
					implode( ', ', $manage_stock_placeholders );

				$wpdb->query( $wpdb->prepare( $manage_stock_sql, ...$manage_stock_values ) );
			}

			// Note: We skip wp_set_object_terms() for product_visibility as it's not critical
			// and would require loading term IDs, which is expensive in bulk
		} finally {
			// Re-add hooks
			if ( $wc_save_removed ) {
				add_action( 'woocommerce_after_product_object_save', array( 'WC_ACF_Sync_Handler', 'handle_wc_save' ), 10, 2 );
			}
			if ( $modulario_removed ) {
				add_action( 'woocommerce_update_product', 'update_product_in_modulario', 10, 1 );
			}
		}
	}

	/**
	 * Bulk update ACF warehouse data for multiple products
	 * Writes directly to postmeta table, bypassing ACF's get_field()/update_field() overhead
	 * This is 10-100x faster than per-product updates
	 *
	 * @param array $updates Array of [product_id => ['quantity' => int, 'price' => float], ...]
	 * @return array Updated warehouse data: [product_id => raw_rows_array, ...] (for reuse in stock status calculation)
	 */
	protected function bulk_update_product_warehouse( array $updates ): array {
		global $wpdb;

		if ( empty( $updates ) ) {
			return array();
		}

		// Load existing warehouse data for all products in one query
		$product_ids   = array_keys( $updates );
		$existing_data = $this->batch_load_acf_warehouse_data( $product_ids );

		$use_transactions = $this->supports_transactions();

		// Start transaction for atomicity (if supported)
		if ( $use_transactions ) {
			$wpdb->query( 'START TRANSACTION' );
		}

		// Store updated warehouse data for reuse (avoids re-reading from DB)
		$updated_warehouse_data = array();

		try {
			$skipped_zero_qty = 0;
			$updated_count    = 0;

			foreach ( $updates as $product_id => $data ) {
				$quantity = $data['quantity'];
				$price    = $data['price'];

				// Skip if both quantity and price are 0 (nothing to update)
				// Note: We update even when quantity is 0 if price > 0 (to update price and clear stock)
				if ( $quantity <= 0 && $price <= 0 ) {
					++$skipped_zero_qty;
					// Keep existing data for stock status calculation
					if ( isset( $existing_data[ $product_id ] ) ) {
						$updated_warehouse_data[ $product_id ] = $existing_data[ $product_id ];
					}
					continue;
				}

				// If quantity is 0 but price > 0, set quantity to 0 explicitly (to clear stock)
				if ( $quantity <= 0 ) {
					$quantity = 0;
				}

				// Get existing rows for this product
				$raw_rows = $existing_data[ $product_id ] ?? array();

				// Find existing row for this supplier ID
				$found_index = null;
				foreach ( $raw_rows as $i => $row ) {
					$row_id = isset( $row[ self::ACF_KEY_ID ] ) ? $row[ self::ACF_KEY_ID ] :
								( isset( $row[ self::ACF_FIELD_NAME_ID ] ) ? $row[ self::ACF_FIELD_NAME_ID ] : '' );
					// Compare as strings to handle both numeric and string IDs
					if ( (string) $row_id === (string) $this->supplier_id ) {
						$found_index = $i;
						break;
					}
				}

				// Debug: Log if supplier ID doesn't match (first occurrence per chunk)
				static $logged_supplier_id_mismatch = false;
				if ( ! $logged_supplier_id_mismatch && $found_index === null && ! empty( $raw_rows ) ) {
					$this->logger->debug(
						sprintf(
							'Supplier ID %d not found in existing warehouse data for product %d. Existing IDs: %s. Will add new row.',
							$this->supplier_id,
							$product_id,
							implode(
								', ',
								array_map(
									function ( $r ) {
										return $r[ self::ACF_KEY_ID ] ?? $r[ self::ACF_FIELD_NAME_ID ] ?? 'N/A';
									},
									$raw_rows
								)
							)
						)
					);
					$logged_supplier_id_mismatch = true;
				}

				// Update existing row or add new one
				if ( $found_index !== null ) {
					$raw_rows[ $found_index ][ self::ACF_KEY_QTY ]   = $quantity;
					$raw_rows[ $found_index ][ self::ACF_KEY_PRICE ] = $price;
				} else {
					$raw_rows[] = array(
						self::ACF_KEY_ID    => $this->supplier_id,
						self::ACF_KEY_QTY   => $quantity,
						self::ACF_KEY_PRICE => $price,
					);
				}

				// Write directly to postmeta (bypass ACF)
				$this->write_acf_warehouse_direct( $product_id, $raw_rows );

				// Store updated data for reuse in stock status calculation
				$updated_warehouse_data[ $product_id ] = $raw_rows;
				++$updated_count;
			}

			if ( $skipped_zero_qty > 0 ) {
				$this->logger->debug( sprintf( 'Skipped %d products with quantity <= 0 in bulk_update_product_warehouse', $skipped_zero_qty ) );
			}
			if ( $updated_count > 0 ) {
				$this->logger->debug( sprintf( 'Updated warehouse data for %d products (supplier_id=%d)', $updated_count, $this->supplier_id ) );
			}

			// Commit transaction (if supported)
			if ( $use_transactions ) {
				$wpdb->query( 'COMMIT' );
			}
			return $updated_warehouse_data;

		} catch ( \Exception $e ) {
			// Rollback on error (if transaction was started)
			if ( $use_transactions ) {
				$wpdb->query( 'ROLLBACK' );
			}
			$this->logger->error( 'Error in bulk_update_product_warehouse: ' . $e->getMessage() );
			throw $e;
		}
	}

	/**
	 * Write ACF repeater data directly to postmeta table
	 * Bypasses ACF's update_field() to avoid overhead
	 *
	 * @param int   $product_id Product ID
	 * @param array $raw_rows Array of repeater rows with ACF keys
	 * @return bool Success status
	 */
	protected function write_acf_warehouse_direct( int $product_id, array $raw_rows ): bool {
		global $wpdb;

		// Delete all existing warehouse meta for this product
		// Use direct query for LIKE pattern (wpdb->delete doesn't support LIKE)
		$wpdb->query(
			$wpdb->prepare(
				"DELETE FROM {$wpdb->postmeta} 
             WHERE post_id = %d 
             AND (meta_key = 'external_warehouse' OR meta_key LIKE 'external_warehouse_%%')",
				$product_id
			)
		);

		if ( empty( $raw_rows ) ) {
			// Update count to 0
			$wpdb->insert(
				$wpdb->postmeta,
				array(
					'post_id'    => $product_id,
					'meta_key'   => 'external_warehouse',
					'meta_value' => '0',
				),
				array( '%d', '%s', '%s' )
			);
			return true;
		}

		// Prepare bulk insert for count + all repeater rows
		$insert_values       = array();
		$insert_placeholders = array();

		// Add count
		$insert_values[]       = $product_id;
		$insert_values[]       = 'external_warehouse';
		$insert_values[]       = (string) count( $raw_rows );
		$insert_placeholders[] = '(%d, %s, %s)';

		// Add each repeater row (3 fields per row: id, qty, price)
		foreach ( $raw_rows as $index => $row ) {
			$warehouse_id = $row[ self::ACF_KEY_ID ] ?? '';
			$qty          = $row[ self::ACF_KEY_QTY ] ?? 0;
			$price        = $row[ self::ACF_KEY_PRICE ] ?? 0.0;

			if ( empty( $warehouse_id ) ) {
				continue;
			}

			// ID field
			$insert_values[]       = $product_id;
			$insert_values[]       = "external_warehouse_{$index}_external_warehouse_id";
			$insert_values[]       = (string) $warehouse_id;
			$insert_placeholders[] = '(%d, %s, %s)';

			// Quantity field
			$insert_values[]       = $product_id;
			$insert_values[]       = "external_warehouse_{$index}_external_warehouse_product_quantity";
			$insert_values[]       = (string) $qty;
			$insert_placeholders[] = '(%d, %s, %s)';

			// Price field
			$insert_values[]       = $product_id;
			$insert_values[]       = "external_warehouse_{$index}_external_warehouse_product_price";
			$insert_values[]       = (string) $price;
			$insert_placeholders[] = '(%d, %s, %s)';
		}

		// Bulk insert all at once
		if ( ! empty( $insert_values ) ) {
			$sql = "INSERT INTO {$wpdb->postmeta} (post_id, meta_key, meta_value) VALUES " .
					implode( ', ', $insert_placeholders );
			$wpdb->query( $wpdb->prepare( $sql, ...$insert_values ) );
		}

		return true;
	}

	/**
	 * Update ACF repeater field for a product (legacy method, kept for backward compatibility)
	 * Similar to wc-warehouse.php lines 782-791
	 * Optimized: Disables expensive hooks during bulk import
	 *
	 * @deprecated Use bulk_update_product_warehouse() for better performance
	 */
	protected function update_product_warehouse( int $product_id, int $quantity, float $price ): bool {
		if ( ! function_exists( 'get_field' ) || ! function_exists( 'update_field' ) ) {
			$this->logger->error( "ACF functions not available for product {$product_id}." );
			return false;
		}

		// Disable expensive hooks during bulk import (like Modulario sync does with CEOD_IMPORTING)
		// This prevents API calls and expensive processing for each product
		if ( ! defined( 'PNEUPEX_SUPPLIER_IMPORTING' ) ) {
			define( 'PNEUPEX_SUPPLIER_IMPORTING', true );
		}

		// Get raw ACF data (with field keys)
		// Use get_field with false flag to get raw data (faster, no formatting)
		$raw_rows = get_field( 'external_warehouse', $product_id, false );

		// Fallback: If ACF get_field fails or is slow, use direct meta query
		if ( ! is_array( $raw_rows ) && function_exists( 'get_post_meta' ) ) {
			// ACF stores repeater count in 'external_warehouse' meta key
			$repeater_count = get_post_meta( $product_id, 'external_warehouse', true );
			$repeater_count = is_numeric( $repeater_count ) ? (int) $repeater_count : 0;

			if ( $repeater_count > 0 ) {
				$raw_rows = array();
				// Reconstruct ACF repeater structure from meta
				for ( $i = 0; $i < $repeater_count; $i++ ) {
					$row                        = array();
					$row[ self::ACF_KEY_ID ]    = get_post_meta( $product_id, "external_warehouse_{$i}_external_warehouse_id", true );
					$row[ self::ACF_KEY_QTY ]   = get_post_meta( $product_id, "external_warehouse_{$i}_external_warehouse_product_quantity", true );
					$row[ self::ACF_KEY_PRICE ] = get_post_meta( $product_id, "external_warehouse_{$i}_external_warehouse_product_price", true );
					if ( ! empty( $row[ self::ACF_KEY_ID ] ) ) {
						$raw_rows[] = $row;
					}
				}
			}
		}

		if ( ! is_array( $raw_rows ) ) {
			$raw_rows = array();
		}

		// Find existing row for this supplier ID
		// Check both field key (raw data) and field name (formatted data)
		$found_index = null;
		foreach ( $raw_rows as $i => $row ) {
			$row_id = isset( $row[ self::ACF_KEY_ID ] ) ? $row[ self::ACF_KEY_ID ] :
						( isset( $row[ self::ACF_FIELD_NAME_ID ] ) ? $row[ self::ACF_FIELD_NAME_ID ] : '' );
			if ( (string) $row_id === (string) $this->supplier_id ) {
				$found_index = $i;
				break;
			}
		}

		// Update existing row or add new one
		if ( $found_index !== null ) {
			// Update using the specific key
			$raw_rows[ $found_index ][ self::ACF_KEY_QTY ]   = $quantity;
			$raw_rows[ $found_index ][ self::ACF_KEY_PRICE ] = $price;

			// Reduced logging during bulk import for performance
			// $this->logger->debug( "Updated existing warehouse row for product {$product_id}, supplier ID {$this->supplier_id}." );
		} else {
			// Add new row - use field names
			$raw_rows[] = array(
				self::ACF_KEY_ID    => $this->supplier_id,
				self::ACF_KEY_QTY   => $quantity,
				self::ACF_KEY_PRICE => $price,
			);
			// Reduced logging during bulk import for performance
			// $this->logger->debug( "Added new warehouse row for product {$product_id}, supplier ID {$this->supplier_id}." );
		}

		// Save back to ACF - update_field handles field name to key conversion
		// Note: update_field() can return false even when successful with repeater fields
		$result = update_field( 'external_warehouse', $raw_rows, $product_id );

		// Skip expensive hooks during bulk import (they can be run later in batch)
		// Modulario sync checks CEOD_IMPORTING, we also check PNEUPEX_SUPPLIER_IMPORTING
		// These API calls are the main bottleneck - disabling them speeds up imports 10-100x
		$is_bulk_import = ( defined( 'PNEUPEX_SUPPLIER_IMPORTING' ) && PNEUPEX_SUPPLIER_IMPORTING ) ||
							( defined( 'CEOD_IMPORTING' ) && CEOD_IMPORTING );

		if ( ! $is_bulk_import ) {
			// Only run these expensive operations if not in bulk import mode
			if ( function_exists( 'update_product_external_warehouse_in_modulario' ) ) {
				update_product_external_warehouse_in_modulario( $product_id );
			}
			if ( function_exists( 'WC_ACF_Sync_Handler' ) && method_exists( 'WC_ACF_Sync_Handler', 'execute_logic' ) ) {
				WC_ACF_Sync_Handler::handle_acf_save( $product_id );
			}
		}

		// // Verify the update actually worked by reading the field back
		// // Use formatted data (field names) for verification, as that's what ACF returns by default
		// $verify_rows = get_field( 'external_warehouse', $product_id );
		// $update_successful = false;
		// $found_row = false;

		// if ( is_array( $verify_rows ) ) {
		// foreach ( $verify_rows as $row ) {
		// Check both field name and field key for ID
		// $row_id = isset( $row[ self::ACF_FIELD_NAME_ID ] ) ? $row[ self::ACF_FIELD_NAME_ID ] :
		// ( isset( $row[ self::ACF_KEY_ID ] ) ? $row[ self::ACF_KEY_ID ] : '' );

		// if ( (string) $row_id === (string) $this->supplier_id ) {
		// $found_row = true;

		// Get values - check both field names and keys
		// $verify_qty = isset( $row[ self::ACF_FIELD_NAME_QTY ] ) ? $row[ self::ACF_FIELD_NAME_QTY ] :
		// ( isset( $row[ self::ACF_KEY_QTY ] ) ? $row[ self::ACF_KEY_QTY ] : 0 );
		// $verify_price = isset( $row[ self::ACF_FIELD_NAME_PRICE ] ) ? $row[ self::ACF_FIELD_NAME_PRICE ] : 0;

		// Convert to same types for comparison (handle float precision)
		// $verify_qty_int = (int) $verify_qty;
		// $verify_price_float = (float) $verify_price;

		// Use approximate comparison for price (float precision)
		// $qty_match = ( $verify_qty_int === $quantity );
		// $price_match = ( abs( $verify_price_float - $price ) < 0.01 );

		// if ( $qty_match && $price_match ) {
		// $update_successful = true;
		// break;
		// } else {
		// Log what we found for debugging
		// $this->logger->debug( "Verification mismatch for product {$product_id}: Found qty={$verify_qty_int} (expected {$quantity}), price={$verify_price_float} (expected {$price})" );
		// }
		// }
		// }
		// }

		// if ( $update_successful ) {
		// $this->logger->info( "Successfully updated warehouse data for product {$product_id} (supplier ID: {$this->supplier_id}, qty: {$quantity}, price: {$price})." );
		// return true;
		// } else {
		// if ( ! $found_row ) {
		// $this->logger->error( "Failed to find supplier ID {$this->supplier_id} in warehouse data for product {$product_id} after update." );
		// } else {
		// update_field returned false or values don't match, but update might still have worked
		// This is common with ACF - it can return false even on success
		// $this->logger->warning( "update_field() verification inconclusive for product {$product_id}. Expected qty: {$quantity}, price: {$price}. Update may have succeeded (ACF can return false even on success)." );
		// }
		// Still return true if update_field was called - ACF often returns false even when successful
		// return ( $result !== false || $found_row );
		// }
		return true;
	}

	/**
	 * Process a single product from CSV data
	 *
	 * @param array    $csv_row CSV row data
	 * @param int|null $product_id Optional product ID (if already known from batch lookup)
	 */
	protected function process_product( array $csv_row, ?int $product_id = null ): void {
		// Extract SKU for logging (needed even when product_id is passed)
		$sku = $this->extract_sku( $csv_row );

		// If product_id not provided, look it up (for backward compatibility)
		if ( $product_id === null ) {
			if ( empty( $sku ) ) {
				return;
			}
			$product_id = $this->get_product_by_sku( $sku );
			if ( ! $product_id ) {
				return;
			}
		}

		$quantity   = $this->extract_quantity( $csv_row );
		$base_price = $this->extract_price( $csv_row );

		if ( $base_price <= 0 ) {
			$this->logger->debug( "Skipping product {$product_id} - invalid price." );
			return;
		}

		$final_price = $this->calculate_price( $base_price, $product_id );

		// Use legacy method (for suppliers that override process_product with custom logic)
		// Note: process_products_chunked() uses bulk_update_product_warehouse() instead
		$this->update_product_warehouse( $product_id, $quantity, $final_price );

		// Reduced logging during bulk import for performance (log only every 10th product)
		++$this->log_counter;
		$is_bulk_import = ( defined( 'PNEUPEX_SUPPLIER_IMPORTING' ) && PNEUPEX_SUPPLIER_IMPORTING ) ||
							( defined( 'CEOD_IMPORTING' ) && CEOD_IMPORTING );

		if ( ! $is_bulk_import || $this->log_counter % 10 === 0 ) {
			$sku_display = ! empty( $sku ) ? $sku : 'N/A';
			$this->logger->info( "Processed product {$product_id} (SKU: {$sku_display}): Qty={$quantity}, Price={$final_price}." );
		}
	}

	/**
	 * Process products in chunks (optimized for large imports)
	 * This method handles memory management, execution time limits, and batch SKU lookups
	 *
	 * @param array $csv_rows Array of CSV row data
	 * @param int   $chunk_size Number of products to process per chunk (default: 50, auto-adjusted for cron/large imports)
	 * @return array Statistics: ['processed' => int, 'updated' => int, 'errors' => int, 'skipped' => int]
	 */
	protected function process_products_chunked( array $csv_rows, int $chunk_size = 50 ): array {
		// Auto-adjust chunk size for cron jobs or large imports
		$is_cron = ( defined( 'DOING_CRON' ) && DOING_CRON ) ||
					( function_exists( 'wp_doing_cron' ) && wp_doing_cron() );

		$total_rows = count( $csv_rows );

		// For cron jobs with large imports (10k+), use larger chunks for efficiency
		if ( $is_cron && $total_rows > 10000 ) {
			$chunk_size = 200; // Larger chunks for very large imports
			$this->logger->info( "Large import detected ({$total_rows} products). Using chunk size: {$chunk_size}" );
		} elseif ( $is_cron && $total_rows > 5000 ) {
			$chunk_size = 100; // Medium chunks for medium imports
			$this->logger->info( "Medium import detected ({$total_rows} products). Using chunk size: {$chunk_size}" );
		}
		$stats = array(
			'processed' => 0,
			'updated'   => 0,
			'errors'    => 0,
			'skipped'   => 0,
		);

		if ( empty( $csv_rows ) ) {
			return $stats;
		}

		// Reset time tracking and extend execution time limit for cron jobs
		$this->reset_time_tracking();
		$this->extend_execution_time();

		// Disable expensive hooks during bulk import (prevents Modulario API calls per product)
		// Use both constants: PNEUPEX_SUPPLIER_IMPORTING (our flag) and CEOD_IMPORTING (Modulario sync flag)
		if ( ! defined( 'PNEUPEX_SUPPLIER_IMPORTING' ) ) {
			define( 'PNEUPEX_SUPPLIER_IMPORTING', true );
		}
		if ( ! defined( 'CEOD_IMPORTING' ) ) {
			define( 'CEOD_IMPORTING', true );
		}

		// Suspend cache addition to prevent memory bloat (like XML feed implementation)
		wp_suspend_cache_addition( true );

		$total_rows = count( $csv_rows );
		$this->logger->info( "Processing {$total_rows} products in chunks of {$chunk_size}..." );

		// Reset log counter for this import
		$this->log_counter = 0;

		// Process in chunks
		$chunks      = array_chunk( $csv_rows, $chunk_size );
		$chunk_count = count( $chunks );

		try {
			foreach ( $chunks as $chunk_index => $chunk ) {
				$chunk_num = $chunk_index + 1;
				$this->logger->info( "Processing chunk {$chunk_num}/{$chunk_count} ({$chunk_size} products)..." );

				// Check memory before processing chunk
				if ( $this->is_memory_limit_approaching() ) {
					$this->logger->warning( "Memory limit approaching. Processed {$stats['processed']} products so far. Stopping to prevent fatal error." );
					break;
				}

				// Extract all SKUs from this chunk
				$skus = array();
				foreach ( $chunk as $row ) {
					$sku = $this->extract_sku( $row );
					if ( ! empty( $sku ) ) {
						$skus[] = $sku;
					}
				}

				// Batch lookup all product IDs for this chunk
				$sku_to_product_id = $this->get_products_by_skus( $skus );

				if ( empty( $sku_to_product_id ) ) {
					$stats['skipped'] += count( $chunk );
					continue;
				}

				// Get all product IDs for this chunk
				$product_ids = array_values( $sku_to_product_id );

				// Batch load all meta for this chunk (reduces N+1 queries)
				update_postmeta_cache( $product_ids );

				// Batch load all term relationships for coefficient calculation (reduces term queries)
				wp_cache_add_non_persistent_groups( array( 'terms', 'term_taxonomy' ) );
				$this->batch_load_product_terms( $product_ids );

				// Collect warehouse updates for bulk processing (replaces per-product ACF calls)
				$warehouse_updates = array();

				// Process each product in chunk
				foreach ( $chunk as $row ) {
					try {
						$sku = $this->extract_sku( $row );
						if ( empty( $sku ) ) {
							++$stats['skipped'];
							continue;
						}

						// Use batch lookup result
						if ( ! isset( $sku_to_product_id[ $sku ] ) ) {
							++$stats['skipped'];
							continue;
						}

						$product_id = $sku_to_product_id[ $sku ];
						$quantity   = $this->extract_quantity( $row );

						// Get base price: from CSV or from product (if supplier overrides get_base_price_from_product)
						$base_price = $this->extract_price( $row );
						if ( $base_price <= 0 ) {
							// Try getting price from product (for suppliers like Goodyear2)
							$base_price = $this->get_base_price_from_product( $product_id, $row );
						}

						if ( $base_price <= 0 ) {
							// Log first few skipped products for debugging
							static $price_skip_logged = 0;
							if ( $price_skip_logged < 3 ) {
								$this->logger->debug(
									sprintf(
										'Skipping product %d (SKU: %s) - price is 0 or negative. CSV price: %s, Product price: %s',
										$product_id,
										$sku,
										$this->extract_price( $row ),
										$this->get_base_price_from_product( $product_id, $row )
									)
								);
								++$price_skip_logged;
							}
							++$stats['skipped'];
							continue;
						}

						$final_price = $this->calculate_price( $base_price, $product_id );

						// Collect for bulk update instead of per-product ACF call
						$warehouse_updates[ $product_id ] = array(
							'quantity' => $quantity,
							'price'    => $final_price,
							'csv_row'  => $row, // Store row for additional meta updates (e.g., Handlopex eprel)
						);

						++$stats['processed'];

					} catch ( \Exception $e ) {
						$this->logger->error( 'Error processing product: ' . $e->getMessage() );
						++$stats['errors'];
					}
				}

				// Bulk update warehouse data (replaces thousands of get_field()/update_field() calls)
				$updated_warehouse_data = array();
				if ( ! empty( $warehouse_updates ) ) {
					$this->logger->debug( sprintf( 'Preparing bulk warehouse update for %d products', count( $warehouse_updates ) ) );

					// Extract just quantity/price for bulk update (remove csv_row)
					$bulk_updates = array();
					foreach ( $warehouse_updates as $product_id => $data ) {
						$bulk_updates[ $product_id ] = array(
							'quantity' => $data['quantity'],
							'price'    => $data['price'],
						);
					}

					$updated_warehouse_data = $this->bulk_update_product_warehouse( $bulk_updates );
					$stats['updated']      += count( $bulk_updates );

					$this->logger->debug(
						sprintf(
							'Bulk warehouse update completed. %d products processed, %d warehouse entries updated',
							count( $bulk_updates ),
							count( $updated_warehouse_data )
						)
					);

					// Allow suppliers to update additional meta fields after bulk warehouse update
					// (e.g., Handlopex updates 'eprel' meta)
					$this->update_additional_product_meta( $warehouse_updates );
				} else {
					$this->logger->warning( 'No warehouse updates collected in this chunk. Check if products are being skipped due to price/quantity validation.' );
				}

				// Bulk update stock status for all products in chunk (replaces WC_ACF_Sync_Handler logic)
				// Pass pre-loaded warehouse data to avoid re-reading from DB
				if ( ! empty( $product_ids ) ) {
					$this->bulk_update_stock_status( $product_ids, $updated_warehouse_data );
				}

				// Track only products that were actually updated (warehouse data changed)
				// This ensures Modulario sync only includes products affected by the import
				if ( ! empty( $warehouse_updates ) ) {
					$updated_ids = array_keys( $warehouse_updates );
					array_push( $this->updated_product_ids, ...$updated_ids );
					$this->logger->debug( sprintf( 'Tracked %d updated product IDs for Modulario sync', count( $updated_ids ) ) );
				}

				// Clear memory after each chunk (targeted, not full flush)
				$this->clear_memory_targeted( $product_ids );

				// Check if we're running out of time (leave 30 seconds buffer)
				if ( $this->is_time_limit_approaching( 30 ) ) {
					$this->logger->warning( "Time limit approaching. Processed {$stats['processed']} products so far. Remaining chunks will be processed in next run." );
					break;
				}
			}
		} finally {
			// Re-enable cache addition
			wp_suspend_cache_addition( false );

			// Re-enable hooks after bulk import completes
			// Note: Modulario sync will need to be run separately if needed
			if ( defined( 'PNEUPEX_SUPPLIER_IMPORTING' ) && PNEUPEX_SUPPLIER_IMPORTING ) {
				// Optionally schedule a background job to sync Modulario updates
				// For now, we just disable the flag - Modulario can sync on next product edit
			}
		}

		return $stats;
	}

	/**
	 * Process products in chunks with custom process_product logic
	 * For suppliers that override process_product() (e.g., Michelin, Continental, Goodyear)
	 * Still uses batch SKU lookup and memory management
	 *
	 * @param array $csv_rows Array of CSV row data
	 * @param int   $chunk_size Number of products to process per chunk (default: 50, auto-adjusted for cron/large imports)
	 * @return array Statistics: ['processed' => int, 'updated' => int, 'errors' => int, 'skipped' => int]
	 */
	protected function process_products_chunked_with_custom_logic( array $csv_rows, int $chunk_size = 50 ): array {
		// Auto-adjust chunk size for cron jobs or large imports
		$is_cron = ( defined( 'DOING_CRON' ) && DOING_CRON ) ||
					( function_exists( 'wp_doing_cron' ) && wp_doing_cron() );

		$total_rows = count( $csv_rows );

		// For cron jobs with large imports (10k+), use larger chunks for efficiency
		if ( $is_cron && $total_rows > 10000 ) {
			$chunk_size = 200; // Larger chunks for very large imports
			$this->logger->info( "Large import detected ({$total_rows} products). Using chunk size: {$chunk_size}" );
		} elseif ( $is_cron && $total_rows > 5000 ) {
			$chunk_size = 100; // Medium chunks for medium imports
			$this->logger->info( "Medium import detected ({$total_rows} products). Using chunk size: {$chunk_size}" );
		}
		$stats = array(
			'processed' => 0,
			'updated'   => 0,
			'errors'    => 0,
			'skipped'   => 0,
		);

		if ( empty( $csv_rows ) ) {
			return $stats;
		}

		// Reset time tracking and extend execution time limit for cron jobs
		$this->reset_time_tracking();
		$this->extend_execution_time();

		// Disable expensive hooks during bulk import (prevents Modulario API calls per product)
		// Use both constants: PNEUPEX_SUPPLIER_IMPORTING (our flag) and CEOD_IMPORTING (Modulario sync flag)
		if ( ! defined( 'PNEUPEX_SUPPLIER_IMPORTING' ) ) {
			define( 'PNEUPEX_SUPPLIER_IMPORTING', true );
		}
		if ( ! defined( 'CEOD_IMPORTING' ) ) {
			define( 'CEOD_IMPORTING', true );
		}

		// Suspend cache addition to prevent memory bloat (like XML feed implementation)
		wp_suspend_cache_addition( true );

		$total_rows = count( $csv_rows );
		$this->logger->info( "Processing {$total_rows} products in chunks of {$chunk_size} (using custom process_product logic)..." );

		// Reset log counter for this import
		$this->log_counter = 0;

		// Process in chunks
		$chunks      = array_chunk( $csv_rows, $chunk_size );
		$chunk_count = count( $chunks );

		try {
			foreach ( $chunks as $chunk_index => $chunk ) {
				$chunk_num = $chunk_index + 1;
				$this->logger->info( "Processing chunk {$chunk_num}/{$chunk_count} ({$chunk_size} products)..." );

				// Check memory before processing chunk
				if ( $this->is_memory_limit_approaching() ) {
					$this->logger->warning( "Memory limit approaching. Processed {$stats['processed']} products so far. Stopping to prevent fatal error." );
					break;
				}

				// Extract all SKUs from this chunk for batch lookup
				$skus = array();
				foreach ( $chunk as $row ) {
					$sku = $this->extract_sku( $row );
					if ( ! empty( $sku ) ) {
						$skus[] = $sku;
					}
				}

				// Batch lookup all product IDs for this chunk
				$sku_to_product_id = $this->get_products_by_skus( $skus );

				if ( empty( $sku_to_product_id ) ) {
					$stats['skipped'] += count( $chunk );
					continue;
				}

				// Get all product IDs for this chunk
				$product_ids = array_values( $sku_to_product_id );

				// Batch load all meta for this chunk (reduces N+1 queries)
				update_postmeta_cache( $product_ids );

				// Batch load all term relationships for coefficient calculation (reduces term queries)
				wp_cache_add_non_persistent_groups( array( 'terms', 'term_taxonomy' ) );
				$this->batch_load_product_terms( $product_ids );

				// Track product IDs that were successfully processed (for Modulario sync)
				$processed_product_ids = array();

				// Process each product in chunk using custom process_product logic
				foreach ( $chunk as $row ) {
					try {
						$sku = $this->extract_sku( $row );
						if ( empty( $sku ) ) {
							++$stats['skipped'];
							continue;
						}

						// Skip if product not found (already filtered by batch lookup)
						if ( ! isset( $sku_to_product_id[ $sku ] ) ) {
							++$stats['skipped'];
							continue;
						}

						$product_id = $sku_to_product_id[ $sku ];

						// Use custom process_product method (may be overridden by supplier)
						// Pass product_id to avoid duplicate SKU lookup
						$this->process_product( $row, $product_id );

						// Track successfully processed product for Modulario sync
						$processed_product_ids[] = $product_id;

						++$stats['processed'];
						++$stats['updated'];

					} catch ( \Exception $e ) {
						$this->logger->error( 'Error processing product: ' . $e->getMessage() );
						++$stats['errors'];
					}
				}

				// Bulk update stock status for all products in chunk (replaces WC_ACF_Sync_Handler logic)
				// This is much more efficient than calling execute_logic() for each product
				if ( ! empty( $product_ids ) ) {
					$this->bulk_update_stock_status( $product_ids );
				}

				// Track only products that were successfully processed (not skipped due to errors)
				if ( ! empty( $processed_product_ids ) ) {
					array_push( $this->updated_product_ids, ...$processed_product_ids );
					$this->logger->debug( sprintf( 'Tracked %d processed product IDs for Modulario sync (custom logic path)', count( $processed_product_ids ) ) );
				}

				// Clear memory after each chunk (targeted, not full flush)
				$this->clear_memory_targeted( $product_ids );

				// Check if we're running out of time (leave 30 seconds buffer)
				if ( $this->is_time_limit_approaching( 30 ) ) {
					$this->logger->warning( "Time limit approaching. Processed {$stats['processed']} products so far. Remaining chunks will be processed in next run." );
					break;
				}
			}
		} finally {
			// Re-enable cache addition
			wp_suspend_cache_addition( false );

			// Re-enable hooks after bulk import completes
			// Note: Modulario sync will need to be run separately if needed
			if ( defined( 'PNEUPEX_SUPPLIER_IMPORTING' ) && PNEUPEX_SUPPLIER_IMPORTING ) {
				// Optionally schedule a background job to sync Modulario updates
				// For now, we just disable the flag - Modulario can sync on next product edit
			}
		}

		return $stats;
	}

	/**
	 * Extend PHP execution time limit for long-running imports
	 * Works in both cron and manual/web contexts
	 * Note: set_time_limit() may be disabled on some servers, but ini_set() usually works
	 * For cron jobs, uses longer time limit (3600s) to allow processing all products
	 */
	protected function extend_execution_time( int $seconds = 600 ): void {
		// Detect if we're running in cron context - use longer time limit
		$is_cron = ( defined( 'DOING_CRON' ) && DOING_CRON ) ||
					( function_exists( 'wp_doing_cron' ) && wp_doing_cron() );

		// For cron jobs, use 1 hour (3600s) to allow processing all products
		// For web requests, use the provided seconds (default 600s = 10 minutes)
		$time_limit = $is_cron ? 3600 : $seconds;

		// Try set_time_limit first (may be disabled on some servers)
		if ( function_exists( 'set_time_limit' ) && ! in_array( 'set_time_limit', explode( ',', ini_get( 'disable_functions' ) ), true ) ) {
			@set_time_limit( $time_limit );
		}

		// Also try ini_set (usually works even if set_time_limit is disabled)
		if ( function_exists( 'ini_set' ) ) {
			@ini_set( 'max_execution_time', (string) $time_limit );
		}

		// Increase memory limit if possible (helps with large imports)
		$current_memory = ini_get( 'memory_limit' );
		if ( $current_memory && function_exists( 'wp_convert_hr_to_bytes' ) ) {
			$current_bytes = wp_convert_hr_to_bytes( $current_memory );
			// Only increase if current limit is less than 512M
			if ( $current_bytes < 536870912 ) { // 512MB in bytes
				@ini_set( 'memory_limit', '512M' );
			}
		}
	}

	/**
	 * Reset time tracking for accurate time limit checks
	 * Should be called at the start of each import
	 */
	protected function reset_time_tracking(): void {
		$this->import_start_time = microtime( true );
	}

	/**
	 * Check if time limit is approaching
	 * Works in both cron and manual/web contexts
	 * For cron jobs, uses more lenient buffer (120s) to allow processing more products
	 */
	protected function is_time_limit_approaching( int $buffer_seconds = 30 ): bool {
		// Detect if we're running in cron context
		$is_cron = ( defined( 'DOING_CRON' ) && DOING_CRON ) ||
					( function_exists( 'wp_doing_cron' ) && wp_doing_cron() );

		// For cron jobs, use larger buffer (120s) and check less frequently
		// This allows processing more products before stopping
		if ( $is_cron ) {
			$buffer_seconds = 120; // 2 minutes buffer for cron
		}

		$max_execution_time = ini_get( 'max_execution_time' );
		if ( $max_execution_time <= 0 ) {
			return false; // No limit
		}

		// Initialize start time if not already set
		if ( $this->import_start_time === null ) {
			$this->import_start_time = microtime( true );
		}

		// Calculate elapsed time
		$elapsed   = microtime( true ) - $this->import_start_time;
		$remaining = $max_execution_time - $elapsed;

		return $remaining < $buffer_seconds;
	}

	/**
	 * Clear memory caches and force garbage collection
	 * Uses targeted cache clearing instead of full flush (more efficient)
	 */
	protected function clear_memory(): void {
		// Clear ACF value store periodically (not every chunk to avoid overhead)
		// Only reset every 10 chunks to balance memory vs performance
		static $acf_reset_counter = 0;
		++$acf_reset_counter;
		if ( $acf_reset_counter % 10 === 0 && function_exists( 'acf_get_store' ) ) {
			acf_get_store( 'values' )->reset();
		}

		// Force garbage collection
		if ( function_exists( 'gc_collect_cycles' ) ) {
			gc_collect_cycles();
		}
	}

	/**
	 * Clear memory for specific products (targeted cache clearing)
	 * More efficient than wp_cache_flush() which clears entire cache
	 *
	 * @param array $product_ids Array of product IDs to clear cache for
	 */
	protected function clear_memory_targeted( array $product_ids ): void {
		// Clear cache for specific products (targeted, not full flush)
		foreach ( $product_ids as $product_id ) {
			clean_post_cache( $product_id );
		}

		// Clear ACF value store periodically (not every chunk to avoid overhead)
		// Only reset every 10 chunks to balance memory vs performance
		static $acf_reset_counter = 0;
		++$acf_reset_counter;
		if ( $acf_reset_counter % 10 === 0 && function_exists( 'acf_get_store' ) ) {
			acf_get_store( 'values' )->reset();
		}

		// Force garbage collection periodically (not every chunk to reduce overhead)
		static $gc_counter = 0;
		++$gc_counter;
		if ( $gc_counter % 5 === 0 && function_exists( 'gc_collect_cycles' ) ) {
			gc_collect_cycles();
		}
	}

	/**
	 * Check if memory limit is approaching (90% threshold)
	 * Prevents fatal errors from memory exhaustion
	 *
	 * @return bool True if memory usage is approaching limit
	 */
	protected function is_memory_limit_approaching(): bool {
		$current_memory = memory_get_usage( true );
		$memory_limit   = ini_get( 'memory_limit' );

		if ( empty( $memory_limit ) || $memory_limit === '-1' ) {
			return false; // No limit
		}

		$memory_limit_bytes = $this->convert_memory_limit_to_bytes( $memory_limit );
		if ( $memory_limit_bytes <= 0 ) {
			return false; // Invalid limit
		}

		$memory_percent = ( $current_memory / $memory_limit_bytes ) * 100;

		// Stop if memory usage exceeds 90% to prevent fatal errors
		if ( $memory_percent > 90 ) {
			$this->logger->warning(
				sprintf(
					'Memory usage at %.1f%% (%.2f MB / %.2f MB). Approaching limit.',
					$memory_percent,
					$current_memory / 1024 / 1024,
					$memory_limit_bytes / 1024 / 1024
				)
			);
			return true;
		}

		return false;
	}

	/**
	 * Convert memory limit string (e.g., "512M") to bytes
	 *
	 * @param string $memory_limit Memory limit string
	 * @return int Memory limit in bytes
	 */
	private function convert_memory_limit_to_bytes( string $memory_limit ): int {
		if ( function_exists( 'wp_convert_hr_to_bytes' ) ) {
			return wp_convert_hr_to_bytes( $memory_limit );
		}

		// Fallback manual conversion
		$memory_limit = trim( $memory_limit );
		$last_char    = strtolower( substr( $memory_limit, -1 ) );
		$value        = (int) $memory_limit;

		switch ( $last_char ) {
			case 'g':
				$value *= 1024;
				// Fall through
			case 'm':
				$value *= 1024;
				// Fall through
			case 'k':
				$value *= 1024;
		}

		return $value;
	}

	/**
	 * Test Modulario sync with specific product IDs
	 * Useful for testing the sync functionality
	 *
	 * @param array $product_ids Product IDs to test sync with (optional, uses tracked IDs if empty)
	 * @return array Statistics: ['synced' => int, 'errors' => int, 'skipped' => int]
	 */
	public function test_modulario_sync( array $product_ids = array() ): array {
		// Initialize logger if not already started (for testing without import)
		if ( ! $this->logger->is_log_open() ) {
			$this->logger->start_log();
		}

		if ( empty( $product_ids ) ) {
			$product_ids = array_unique( $this->updated_product_ids );
		}

		if ( empty( $product_ids ) ) {
			$this->logger->error( 'No product IDs provided for testing. Use test_modulario_sync([123, 456, ...]) or run an import first.' );
			return array(
				'synced'  => 0,
				'errors'  => 0,
				'skipped' => 0,
			);
		}

		$this->logger->info(
			sprintf(
				'Testing Modulario sync with %d product IDs: %s',
				count( $product_ids ),
				implode( ', ', array_slice( $product_ids, 0, 10 ) ) . ( count( $product_ids ) > 10 ? '...' : '' )
			)
		);

		// Temporarily store original updated_product_ids
		$original_ids = $this->updated_product_ids;

		// Set test product IDs
		$this->updated_product_ids = $product_ids;

		// Run sync
		$stats = $this->sync_updated_products_to_modulario();

		// Restore original IDs
		$this->updated_product_ids = $original_ids;

		return $stats;
	}

	/**
	 * Sync updated products to Modulario after import
	 * Called automatically after import finishes
	 * Can also be called with specific product IDs for cross-supplier sync
	 *
	 * @param array|null $product_ids Optional: specific product IDs to sync. If null, uses tracked IDs.
	 * @return array Statistics: ['synced' => int, 'errors' => int, 'skipped' => int]
	 */
	public function sync_updated_products_to_modulario( ?array $product_ids = null ): array {
		$this->logger->info( '=== Modulario sync started ===' );

		if ( ! function_exists( 'call_modulario' ) ) {
			$this->logger->error( 'Modulario sync function not available. Skipping sync.' );
			return array(
				'synced'  => 0,
				'errors'  => 0,
				'skipped' => 0,
			);
		}

		// Use provided product IDs or fall back to tracked IDs
		if ( $product_ids === null ) {
			$product_ids = $this->updated_product_ids;
			$this->logger->debug( sprintf( 'Tracked product IDs count: %d', count( $product_ids ) ) );
		} else {
			$this->logger->debug( sprintf( 'Using provided product IDs count: %d', count( $product_ids ) ) );
		}

		// Get unique product IDs
		$product_ids = array_unique( $product_ids );

		$this->logger->debug( sprintf( 'Unique product IDs after deduplication: %d', count( $product_ids ) ) );

		if ( empty( $product_ids ) ) {
			$this->logger->info( 'No products were updated. Skipping Modulario sync.' );
			return array(
				'synced'  => 0,
				'errors'  => 0,
				'skipped' => 0,
			);
		}

		$this->logger->info( sprintf( 'Starting Modulario sync for %d updated products.', count( $product_ids ) ) );

		// Load supplier ID to name and UID mappings once per sync run (not per batch)
		$mapper               = new Pneupex_Supplier_ID_Mapper();
		$supplier_id_to_name  = $mapper->get_id_to_name_mapping();
		$supplier_uid_mapping = $mapper->get_uid_mapping();

		// Get batch size
		$batch_size = (int) apply_filters( 'pneupex_modulario_sync_batch_size', self::MODULARIO_SYNC_BATCH_SIZE );

		// Process in batches
		$batches = array_chunk( $product_ids, $batch_size );
		$stats   = array(
			'synced'  => 0,
			'errors'  => 0,
			'skipped' => 0,
		);

		foreach ( $batches as $batch_index => $batch ) {
			$this->logger->info(
				sprintf(
					'Processing Modulario sync batch %d/%d (%d products)',
					$batch_index + 1,
					count( $batches ),
					count( $batch )
				)
			);

			$batch_stats       = $this->sync_products_batch_to_modulario( $batch, $supplier_id_to_name, $supplier_uid_mapping );
			$stats['synced']  += $batch_stats['synced'];
			$stats['errors']  += $batch_stats['errors'];
			$stats['skipped'] += $batch_stats['skipped'];

			// Rate limiting is handled in apply_rate_limiting() before each API call
			// No additional delay needed here
		}

		$this->logger->info(
			sprintf(
				'Modulario sync completed: %d synced, %d errors, %d skipped',
				$stats['synced'],
				$stats['errors'],
				$stats['skipped']
			)
		);

		return $stats;
	}


	/**
	 * Sync a batch of products to Modulario
	 * Efficiently loads data without using expensive WooCommerce methods
	 * Uses Modulario's batch endpoint: update_products with ['products' => [...]]
	 *
	 * @param array $product_ids          Product IDs to sync
	 * @param array $supplier_id_to_name  Pre-loaded supplier ID to name mapping (ID => name)
	 * @param array $supplier_uid_mapping Pre-loaded supplier ID => Modulario _uid mapping
	 * @return array Statistics: ['synced' => int, 'errors' => int, 'skipped' => int]
	 */
	private function sync_products_batch_to_modulario( array $product_ids, array $supplier_id_to_name = array(), array $supplier_uid_mapping = array() ): array {
		$this->logger->debug( sprintf( 'sync_products_batch_to_modulario called with %d product IDs', count( $product_ids ) ) );

		if ( empty( $product_ids ) ) {
			return array(
				'synced'  => 0,
				'errors'  => 0,
				'skipped' => 0,
			);
		}

		// Batch load modulario_id and external_warehouse data
		$product_data = $this->batch_load_product_data_for_modulario( $product_ids );
		$this->logger->debug( sprintf( 'Loaded data for %d products', count( $product_data ) ) );

		// If mappings not provided, load them (fallback for direct calls)
		if ( empty( $supplier_id_to_name ) ) {
			$mapper               = new Pneupex_Supplier_ID_Mapper();
			$supplier_id_to_name  = $mapper->get_id_to_name_mapping();
			$supplier_uid_mapping = $mapper->get_uid_mapping();

		}

		$stats            = array(
			'synced'  => 0,
			'errors'  => 0,
			'skipped' => 0,
		);
		$products_payload = array();

		foreach ( $product_ids as $product_id ) {
			$data = $product_data[ $product_id ] ?? null;

			if ( ! $data || empty( $data['modulario_id'] ) ) {
				++$stats['skipped'];
				$this->logger->debug( sprintf( 'Skipping product %d - missing modulario_id or data', $product_id ) );
				continue;
			}

			// Build per-product payload (same structure as update_product_external_warehouse_in_modulario)
			$product_payload = array(
				'_id' => $data['modulario_id'],
			);

			if ( ! empty( $data['external_warehouse'] ) ) {
				$product_payload['externeSklady'] = array();
				foreach ( $data['external_warehouse'] as $ext ) {
					$supplier_id   = (int) ( $ext['external_warehouse_id'] ?? 0 );
					$supplier_name = $supplier_id_to_name[ $supplier_id ] ?? '';
					$supplier_uid  = $supplier_uid_mapping[ $supplier_id ] ?? '';

					$product_payload['externeSklady'][] = array(
						'externySklad' => array_filter(
							array(
								'_id'        => $supplier_id,
								'_name'      => $supplier_name,
								'internyKod' => $supplier_id,
								'_uid'       => $supplier_uid,
							),
							static function ( $value ) {
								// Keep numeric zero, but drop empty strings / nulls
								return ( is_numeric( $value ) && (int) $value === 0 ) || ! empty( $value );
							}
						),
						'pocet'        => isset( $ext['external_warehouse_product_quantity'] ) ? (int) $ext['external_warehouse_product_quantity'] : 0,
						'cena'         => isset( $ext['external_warehouse_product_price'] ) ? (float) $ext['external_warehouse_product_price'] : 0.0,
					);
				}
			}

			$products_payload[] = $product_payload;
		}

		// If nothing to send (e.g. all skipped due to missing modulario_id), return early
		if ( empty( $products_payload ) ) {
			$this->logger->warning( sprintf( 'No products to sync in batch (all %d products skipped)', count( $product_ids ) ) );
			return $stats;
		}

		$this->logger->info( sprintf( 'Sending batch of %d products to Modulario API', count( $products_payload ) ) );

		// Apply rate limiting before API call
		$this->apply_rate_limiting();

		// Call Modulario batch API: update_products
		$response = call_modulario(
			'update_products',
			array(
				'products' => $products_payload,
			)
		);
		$this->logger->debug( 'Modulario response: ' . print_r( $response, true ) );

		if ( is_wp_error( $response ) ) {
			$this->logger->error(
				sprintf(
					'Failed to batch-sync %d products to Modulario: %s',
					count( $products_payload ),
					$response->get_error_message()
				)
			);
			// Consider all attempted products as errors in this batch
			$stats['errors'] += count( $products_payload );
		} else {
			$stats['synced'] += count( $products_payload );

			// Reduced logging - only log when a batch is successfully synced
			$this->logger->debug(
				sprintf(
					'Batch-synced %d products to Modulario via update_products',
					count( $products_payload )
				)
			);
		}

		return $stats;
	}

	/**
	 * Batch load product data needed for Modulario sync
	 * Efficiently loads modulario_id and external_warehouse without using WooCommerce objects
	 *
	 * @param array $product_ids Product IDs
	 * @return array Array of [product_id => ['modulario_id' => string, 'external_warehouse' => array], ...]
	 */
	private function batch_load_product_data_for_modulario( array $product_ids ): array {
		global $wpdb;

		if ( empty( $product_ids ) ) {
			return array();
		}

		$results      = array();
		$placeholders = implode( ',', array_fill( 0, count( $product_ids ), '%d' ) );

		// Batch load modulario_id
		$modulario_query   = $wpdb->prepare(
			"SELECT post_id, meta_value 
             FROM {$wpdb->postmeta} 
             WHERE post_id IN ($placeholders) 
             AND meta_key = 'modulario_id'",
			...$product_ids
		);
		$modulario_results = $wpdb->get_results( $modulario_query, ARRAY_A );

		// Initialize results array
		foreach ( $product_ids as $product_id ) {
			$results[ $product_id ] = array(
				'modulario_id'       => '',
				'external_warehouse' => array(),
			);
		}

		// Map modulario_id
		foreach ( $modulario_results as $row ) {
			$results[ $row['post_id'] ]['modulario_id'] = $row['meta_value'];
		}

		// Batch load external_warehouse data (reuse existing method)
		$warehouse_data = $this->batch_load_acf_warehouse_data( $product_ids );

		// Map external_warehouse data
		foreach ( $warehouse_data as $product_id => $raw_rows ) {
			if ( empty( $raw_rows ) ) {
				continue;
			}

			$ext_warehouse = array();
			foreach ( $raw_rows as $row ) {
				$warehouse_id = isset( $row[ self::ACF_KEY_ID ] ) ? $row[ self::ACF_KEY_ID ] :
								( isset( $row[ self::ACF_FIELD_NAME_ID ] ) ? $row[ self::ACF_FIELD_NAME_ID ] : '' );

				if ( empty( $warehouse_id ) ) {
					continue;
				}

				$qty   = isset( $row[ self::ACF_KEY_QTY ] ) ? (int) $row[ self::ACF_KEY_QTY ] :
						( isset( $row[ self::ACF_FIELD_NAME_QTY ] ) ? (int) $row[ self::ACF_FIELD_NAME_QTY ] : 0 );
				$price = isset( $row[ self::ACF_KEY_PRICE ] ) ? (float) $row[ self::ACF_KEY_PRICE ] :
						( isset( $row[ self::ACF_FIELD_NAME_PRICE ] ) ? (float) $row[ self::ACF_FIELD_NAME_PRICE ] : 0.0 );

				$ext_warehouse[] = array(
					'external_warehouse_id'               => $warehouse_id,
					'external_warehouse_product_quantity' => $qty,
					'external_warehouse_product_price'    => $price,
				);
			}

			$results[ $product_id ]['external_warehouse'] = $ext_warehouse;
		}

		return $results;
	}

	/**
	 * Extract SKU from CSV row (must be implemented by each supplier)
	 */
	abstract protected function extract_sku( array $row ): string;

	/**
	 * Extract quantity from CSV row (must be implemented by each supplier)
	 */
	abstract protected function extract_quantity( array $row ): int;

	/**
	 * Extract price from CSV row (must be implemented by each supplier)
	 */
	abstract protected function extract_price( array $row ): float;
}
