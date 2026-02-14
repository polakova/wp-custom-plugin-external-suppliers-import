<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

/**
 * Manages price coefficients (markups) for suppliers
 * Uses eshop_suppliers_coefficients database table
 */
class Pneupex_Coefficient_Manager {

	private const TABLE_NAME   = 'eshop_suppliers_coefficients';
	private const CACHE_GROUP  = 'pneupex_coefficients';
	private const CACHE_EXPIRY = 3600; // 1 hour

	/**
	 * Get coefficient for a supplier/product combination
	 * Logic matches original: supplier DESC, TYPE DESC, brand DESC
	 *
	 * @param string $supplier Supplier name
	 * @param string $type Product type (can be empty for wildcard)
	 * @param string $brand Product brand (can be empty for wildcard)
	 * @return float Coefficient value (defaults to 1.0 if no match)
	 */
	public function get_coefficient( string $supplier, string $type = '', string $brand = '' ): float {
		global $wpdb;

		// Get cache version for invalidation support
		$cache_version = (int) wp_cache_get( 'version', self::CACHE_GROUP );
		if ( ! $cache_version ) {
			$cache_version = 1;
			wp_cache_set( 'version', $cache_version, self::CACHE_GROUP, 0 );
		}

		// Check cache first (with version check for invalidation)
		$cache_key = sprintf( 'coef_%s_%s_%s_v%d', md5( $supplier ), md5( $type ), md5( $brand ), $cache_version );
		$cached    = wp_cache_get( $cache_key, self::CACHE_GROUP );
		if ( $cached !== false ) {
			return (float) $cached;
		}

		// Table exists without WordPress prefix (custom table)
		$table_name = self::TABLE_NAME;

		// Query directly with supplier, type, and brand (case-insensitive)
		// Empty type/brand in DB means wildcard (matches anything)
		// Order by specificity: type+brand match > type only > brand only > wildcard

		// Build WHERE conditions
		// For type: (DB type is empty OR matches product type) - empty in DB = wildcard
		// For brand: (DB brand is empty OR matches product brand) - empty in DB = wildcard
		$where_parts  = array( 'LOWER(supplier) = LOWER(%s)' );
		$where_values = array( $supplier );

		// Type condition: DB type empty (wildcard) OR case-insensitive match
		if ( ! empty( $type ) ) {
			$where_parts[]  = "(type = '' OR LOWER(type) = LOWER(%s))";
			$where_values[] = $type;
		} else {
			// If product type is empty, only match DB entries where type is empty
			$where_parts[] = "type = ''";
		}

		// Brand condition: DB brand empty (wildcard) OR case-insensitive match
		if ( ! empty( $brand ) ) {
			$where_parts[]  = "(brand = '' OR LOWER(brand) = LOWER(%s))";
			$where_values[] = $brand;
		} else {
			// If product brand is empty, only match DB entries where brand is empty
			$where_parts[] = "brand = ''";
		}

		$where_clause = implode( ' AND ', $where_parts );

		// Order by specificity: most specific first
		// Priority: type+brand both match > type only > brand only > both empty
		$sql = $wpdb->prepare(
			"SELECT coefficient 
             FROM {$table_name} 
             WHERE {$where_clause}
             ORDER BY 
                 CASE 
                     WHEN type != '' AND brand != '' THEN 1  -- Most specific: both match
                     WHEN type != '' AND brand = '' THEN 2    -- Type only
                     WHEN type = '' AND brand != '' THEN 3    -- Brand only
                     ELSE 4                                   -- Wildcard: both empty
                 END
             LIMIT 1",
			...$where_values
		);

		$coefficient = $wpdb->get_var( $sql );

		// If nothing found, return default
		if ( $coefficient === null ) {
			wp_cache_set( $cache_key, 1.0, self::CACHE_GROUP, self::CACHE_EXPIRY );
			return 1.0;
		}

		$coefficient = (float) $coefficient;

		// Cache the result
		wp_cache_set( $cache_key, $coefficient, self::CACHE_GROUP, self::CACHE_EXPIRY );

		return $coefficient;
	}

	/**
	 * Get all coefficients from database
	 *
	 * @return array Array of coefficient records
	 */
	public function get_all_coefficients(): array {
		global $wpdb;

		// Check cache
		$cache_key = 'all_coefficients';
		$cached    = wp_cache_get( $cache_key, self::CACHE_GROUP );
		if ( $cached !== false ) {
			return $cached;
		}

		// Table exists without WordPress prefix (custom table)
		$table_name = self::TABLE_NAME;

		$sql          = "SELECT id, supplier, type, brand, coefficient FROM {$table_name} ORDER BY supplier, type, brand";
		$coefficients = $wpdb->get_results( $sql, ARRAY_A );

		// Normalize to match old format for backward compatibility
		$normalized = array();
		foreach ( $coefficients as $coef ) {
			$normalized[] = array(
				'id'          => (int) $coef['id'],
				'stock'       => $coef['supplier'], // Keep 'stock' key for backward compatibility
				'supplier'    => $coef['supplier'],
				'type'        => $coef['type'],
				'brand'       => $coef['brand'],
				'coefficient' => (float) $coef['coefficient'],
			);
		}

		wp_cache_set( $cache_key, $normalized, self::CACHE_GROUP, self::CACHE_EXPIRY );

		return $normalized;
	}
}
