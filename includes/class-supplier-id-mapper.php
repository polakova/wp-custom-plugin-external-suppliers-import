<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

/**
 * Maps supplier names to numeric IDs and Modulario UIDs
 * Single source of truth for all supplier mapping data
 */
class Pneupex_Supplier_ID_Mapper {

	private const OPTION_KEY = 'pneupex_supplier_mapping';

	/**
	 * Default supplier data structure
	 * Single canonical source: [id => ['name' => string, 'uid' => string], ...]
	 */
	private array $default_suppliers = array(
		1  => array(
			'name' => 'Handlopex',
			'uid'  => '68babc6770df45e41adc5d0e',
		),
		2  => array(
			'name' => 'Vredestein',
			'uid'  => '68babc7470df45e41adc5d0f',
		),
		3  => array(
			'name' => 'Goodyear',
			'uid'  => '698359811657c0b4f0d1668c',
		),
		4  => array(
			'name' => 'Alcar',
			'uid'  => '698359a31657c0b4f0d166a4',
		),
		5  => array(
			'name' => 'VandenbanCZ',
			'uid'  => '698359ba1657c0b4f0d166b6',
		),
		6  => array(
			'name' => 'VandenbanNL',
			'uid'  => '698359d11657c0b4f0d166c7',
		),
		7  => array(
			'name' => 'GPD',
			'uid'  => '698359e71657c0b4f0d166d8',
		),
		8  => array(
			'name' => 'Continental',
			'uid'  => '698359fb1657c0b4f0d166e9',
		),
		9  => array(
			'name' => 'Bridgestone',
			'uid'  => '69835a0c1657c0b4f0d166f8',
		),
		10 => array(
			'name' => 'Goodyear2-Nemecko',
			'uid'  => '69835a271657c0b4f0d1670d',
		),
		11 => array(
			'name' => 'Michelin',
			'uid'  => '69835a3b1657c0b4f0d1671e',
		),
		12 => array(
			'name' => 'Latex',
			'uid'  => '69835a4f1657c0b4f0d1672f',
		),
		13 => array(
			'name' => 'IHLE',
			'uid'  => '69835a641657c0b4f0d1673e',
		),
		14 => array(
			'name' => 'DobrePneu.cz',
			'uid'  => '69835a7f1657c0b4f0d1674c',
		),
		15 => array(
			'name' => 'Asteel',
			'uid'  => '69835a951657c0b4f0d1675f',
		),
	);

	/**
	 * Cached reverse lookups (lazy-loaded)
	 */
	private ?array $name_to_id_cache = null;
	private ?array $id_to_name_cache = null;
	private ?array $id_to_uid_cache  = null;

	/**
	 * Get all supplier data (id => [name, uid])
	 */
	private function get_suppliers(): array {
		$saved = get_option( self::OPTION_KEY, array() );

		// Use + operator instead of array_merge to preserve numeric keys
		// This ensures keys 1-15 are preserved, not re-indexed to 0-14
		$suppliers = $saved + $this->default_suppliers;

		// Validate structure
		foreach ( $suppliers as $id => $data ) {
			if ( ! is_array( $data ) || ! isset( $data['name'] ) || ! isset( $data['uid'] ) ) {
				unset( $suppliers[ $id ] ); // Remove invalid entries
			}
		}

		// Ensure keys are preserved (ksort to maintain order)
		ksort( $suppliers, SORT_NUMERIC );

		// Allow filter override
		return apply_filters( 'pneupex_supplier_mapping', $suppliers );
	}

	/**
	 * Get cached name => id mapping
	 */
	private function get_name_to_id(): array {
		if ( $this->name_to_id_cache === null ) {
			$this->name_to_id_cache = array();
			foreach ( $this->get_suppliers() as $id => $data ) {
				$this->name_to_id_cache[ $data['name'] ] = (int) $id;
			}
		}
		return $this->name_to_id_cache;
	}

	/**
	 * Get cached id => name mapping
	 */
	private function get_id_to_name(): array {
		if ( $this->id_to_name_cache === null ) {
			$this->id_to_name_cache = array();
			foreach ( $this->get_suppliers() as $id => $data ) {
				$this->id_to_name_cache[ (int) $id ] = $data['name'];
			}
		}
		return $this->id_to_name_cache;
	}

	/**
	 * Get cached id => uid mapping
	 */
	private function get_id_to_uid(): array {
		if ( $this->id_to_uid_cache === null ) {
			$this->id_to_uid_cache = array();
			foreach ( $this->get_suppliers() as $id => $data ) {
				$this->id_to_uid_cache[ (int) $id ] = $data['uid'];
			}
		}
		return $this->id_to_uid_cache;
	}

	/**
	 * Get supplier ID by name
	 */
	public function get_supplier_id( string $supplier_name ): int {
		$mapping = $this->get_name_to_id();
		return $mapping[ $supplier_name ] ?? 0;
	}

	/**
	 * Get supplier name by ID
	 */
	public function get_supplier_name( int $supplier_id ): string {
		$mapping = $this->get_id_to_name();
		return $mapping[ $supplier_id ] ?? '';
	}

	/**
	 * Get Modulario UID by supplier ID
	 */
	public function get_supplier_uid( int $supplier_id ): string {
		$mapping = $this->get_id_to_uid();
		return $mapping[ $supplier_id ] ?? '';
	}

	/**
	 * Get all supplier data (for backward compatibility and admin UI)
	 * Returns: [name => id, ...]
	 */
	public function get_mapping(): array {
		return $this->get_name_to_id();
	}

	/**
	 * Get id => name mapping (optimized for Modulario sync)
	 */
	public function get_id_to_name_mapping(): array {
		return $this->get_id_to_name();
	}

	/**
	 * Get id => uid mapping (optimized for Modulario sync)
	 */
	public function get_uid_mapping(): array {
		return $this->get_id_to_uid();
	}

	/**
	 * Get all supplier names
	 */
	public function get_all_suppliers(): array {
		return array_keys( $this->get_name_to_id() );
	}

	/**
	 * Update UID mapping (for one-time setup scripts)
	 * Accepts [id => uid] format
	 */
	public function update_uid_mapping( array $uid_mapping ): bool {
		// Convert to new format
		$suppliers = $this->get_suppliers();
		foreach ( $uid_mapping as $id => $uid ) {
			if ( isset( $suppliers[ $id ] ) ) {
				$suppliers[ $id ]['uid'] = (string) $uid;
			}
		}
		$this->name_to_id_cache = null; // Clear cache
		$this->id_to_name_cache = null;
		$this->id_to_uid_cache  = null;
		return update_option( self::OPTION_KEY, $suppliers );
	}
}
