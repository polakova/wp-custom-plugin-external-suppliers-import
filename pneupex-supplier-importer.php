<?php
/**
 * Plugin Name: Pneupex Supplier Importer
 * Plugin URI: https://github.com/polakova/wp-custom-plugin-external-suppliers-import
 * Description: Import external supplier stock data into WooCommerce products via ACF repeater fields and synchronise them with Modulario system.
 * Version: 1.0.0
 * Author: Táňa Poláková
 * Author URI: https://github.com/polakova
 * License: GPL v2 or later
 * License URI: https://www.gnu.org/licenses/gpl-2.0.html
 * Text Domain: pneupex-supplier-importer
 * Domain Path: /languages
 * Requires at least: 5.8
 * Requires PHP: 7.4
 * WC requires at least: 5.0
 * WC tested up to: 8.0
 */

declare(strict_types=1);


if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

// Define plugin constants
define( 'PNEUPEX_SUPPLIER_IMPORTER_VERSION', '1.0.0' );
define( 'PNEUPEX_SUPPLIER_IMPORTER_PATH', plugin_dir_path( __FILE__ ) );
define( 'PNEUPEX_SUPPLIER_IMPORTER_URL', plugin_dir_url( __FILE__ ) );

/**
 * Main plugin class
 */
class Pneupex_Supplier_Importer {

	/**
	 * Singleton instance
	 */
	private static ?Pneupex_Supplier_Importer $instance = null;

	/**
	 * Get singleton instance
	 */
	public static function get_instance(): Pneupex_Supplier_Importer {
		if ( self::$instance === null ) {
			self::$instance = new self();
		}
		return self::$instance;
	}

	/**
	 * Constructor
	 */
	private function __construct() {
		$this->load_dependencies();
		$this->init_hooks();
	}

	/**
	 * Load required files
	 */
	private function load_dependencies(): void {
		$includes_dir = PNEUPEX_SUPPLIER_IMPORTER_PATH . 'includes/';

		require_once $includes_dir . 'class-supplier-logger.php';
		require_once $includes_dir . 'class-supplier-id-mapper.php';
		require_once $includes_dir . 'class-coefficient-manager.php';
		require_once $includes_dir . 'class-ftp-handler.php';
		require_once $includes_dir . 'class-supplier-importer-base.php';
		require_once $includes_dir . 'class-cleanup-manager.php';

		// Load supplier classes (can be disabled via constant for debugging)
		if ( ! defined( 'PNEUPEX_DISABLE_SUPPLIER_CLASSES' ) || ! PNEUPEX_DISABLE_SUPPLIER_CLASSES ) {
			$suppliers_dir = $includes_dir . 'suppliers/';
			if ( is_dir( $suppliers_dir ) ) {
				$files = glob( $suppliers_dir . 'class-*-importer.php' );
				if ( $files !== false ) {
					// Sort files to ensure consistent loading order
					sort( $files );
					foreach ( $files as $file ) {
						if ( is_file( $file ) ) {
							// Use output buffering to catch parse errors
							ob_start();
							$error_occurred = false;
							try {
								require_once $file;
							} catch ( \ParseError $e ) {
								$error_occurred = true;
								error_log( '[PNEUPEX_SUPPLIER_IMPORTER] Parse error in file: ' . $file . ' - ' . $e->getMessage() . ' on line ' . $e->getLine() );
							} catch ( \Throwable $e ) {
								$error_occurred = true;
								error_log( '[PNEUPEX_SUPPLIER_IMPORTER] Error loading supplier file: ' . $file . ' - ' . $e->getMessage() . ' in ' . $e->getFile() . ':' . $e->getLine() );
							}
							$output = ob_get_clean();
							if ( $error_occurred || ! empty( $output ) ) {
								if ( ! empty( $output ) ) {
									error_log( '[PNEUPEX_SUPPLIER_IMPORTER] Output from file: ' . $file . ' - ' . $output );
								}
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Initialize hooks
	 */
	private function init_hooks(): void {
		// Admin UI
		add_action( 'admin_menu', array( $this, 'add_admin_menu' ) );
		add_action( 'admin_post_pneupex_run_import', array( $this, 'handle_manual_import' ) );
		add_action( 'admin_post_pneupex_save_schedule', array( $this, 'handle_save_schedule' ) );
		add_action( 'admin_post_pneupex_test_modulario_sync', array( $this, 'handle_test_modulario_sync' ) );

		// Cron
		add_action( 'pneupex_supplier_import_cron', array( $this, 'run_cron_import' ) );
		// Schedule cron only in admin area (not on every frontend page load)
		// This reduces database queries and potential issues
		add_action( 'admin_init', array( $this, 'maybe_schedule_cron' ), 20 );

		// Cleanup - run daily
		add_action( 'pneupex_supplier_cleanup', array( $this, 'run_cleanup' ) );
		// Schedule cleanup only in admin area (not on every frontend page load)
		add_action( 'admin_init', array( $this, 'maybe_schedule_cleanup' ), 20 );

		// Cleanup temp files on shutdown (safety net) - DISABLED by default to avoid issues
		// Can be enabled via filter: add_filter( 'pneupex_enable_shutdown_cleanup', '__return_true' );
		if ( apply_filters( 'pneupex_enable_shutdown_cleanup', false ) && is_admin() ) {
			add_action( 'shutdown', array( $this, 'cleanup_old_temp_files' ), 999 );
		}
	}

	/**
	 * Add admin menu
	 */
	public function add_admin_menu(): void {
		add_menu_page(
			__( 'Supplier Importer', 'pneupex-supplier-importer' ),
			__( 'Supplier Import', 'pneupex-supplier-importer' ),
			'manage_woocommerce',
			'pneupex-supplier-importer',
			array( $this, 'render_admin_page' ),
			'dashicons-update',
			56
		);
	}

	/**
	 * Render admin page
	 */
	public function render_admin_page(): void {
		$mapper    = new Pneupex_Supplier_ID_Mapper();
		$suppliers = $mapper->get_all_suppliers();

		// Show success messages
		$imported = isset( $_GET['imported'] ) ? sanitize_text_field( wp_unslash( $_GET['imported'] ) ) : '';
		if ( $imported === '1' ) {
			$processed        = isset( $_GET['processed'] ) ? absint( $_GET['processed'] ) : 0;
			$updated          = isset( $_GET['updated'] ) ? absint( $_GET['updated'] ) : 0;
			$errors           = isset( $_GET['errors'] ) ? absint( $_GET['errors'] ) : 0;
			$modulario_synced = isset( $_GET['modulario_synced'] ) ? absint( $_GET['modulario_synced'] ) : 0;
			$modulario_errors = isset( $_GET['modulario_errors'] ) ? absint( $_GET['modulario_errors'] ) : 0;
			?>
			<div class="notice notice-success is-dismissible">
				<p><strong><?php esc_html_e( 'Import completed successfully!', 'pneupex-supplier-importer' ); ?></strong></p>
				<ul style="margin: 0.5em 0; padding-left: 2em;">
					<li><?php printf( esc_html__( 'Products processed: %d', 'pneupex-supplier-importer' ), $processed ); ?></li>
					<li><?php printf( esc_html__( 'Products updated: %d', 'pneupex-supplier-importer' ), $updated ); ?></li>
					<li><?php printf( esc_html__( 'Errors: %d', 'pneupex-supplier-importer' ), $errors ); ?></li>
					<?php if ( $modulario_synced > 0 || $modulario_errors > 0 ) : ?>
						<li>
							<strong><?php esc_html_e( 'Modulario sync:', 'pneupex-supplier-importer' ); ?></strong>
							<?php printf( esc_html__( '%d synced', 'pneupex-supplier-importer' ), $modulario_synced ); ?>
							<?php if ( $modulario_errors > 0 ) : ?>
								, <?php printf( esc_html__( '%d errors', 'pneupex-supplier-importer' ), $modulario_errors ); ?>
							<?php endif; ?>
						</li>
					<?php endif; ?>
				</ul>
				<p class="description">
					<?php esc_html_e( 'Stock status has been updated for all affected products. Check logs for detailed information.', 'pneupex-supplier-importer' ); ?>
				</p>
			</div>
			<?php
		}

		$schedule_saved = isset( $_GET['schedule_saved'] ) ? sanitize_text_field( wp_unslash( $_GET['schedule_saved'] ) ) : '';
		if ( $schedule_saved === '1' ) {
			?>
			<div class="notice notice-success is-dismissible">
				<p><?php esc_html_e( 'Schedule saved successfully.', 'pneupex-supplier-importer' ); ?></p>
			</div>
			<?php
		}

		// Show test sync results
		$test_sync = isset( $_GET['test_sync'] ) ? sanitize_text_field( wp_unslash( $_GET['test_sync'] ) ) : '';
		if ( $test_sync === '1' ) {
			$synced  = isset( $_GET['synced'] ) ? absint( $_GET['synced'] ) : 0;
			$errors  = isset( $_GET['errors'] ) ? absint( $_GET['errors'] ) : 0;
			$skipped = isset( $_GET['skipped'] ) ? absint( $_GET['skipped'] ) : 0;
			?>
			<div class="notice notice-<?php echo $errors > 0 ? 'error' : ( $synced > 0 ? 'success' : 'warning' ); ?> is-dismissible">
				<p><strong><?php esc_html_e( 'Modulario Sync Test Results', 'pneupex-supplier-importer' ); ?></strong></p>
				<ul style="margin: 0.5em 0; padding-left: 2em;">
					<li><?php printf( esc_html__( 'Synced: %d', 'pneupex-supplier-importer' ), $synced ); ?></li>
					<li><?php printf( esc_html__( 'Errors: %d', 'pneupex-supplier-importer' ), $errors ); ?></li>
					<li><?php printf( esc_html__( 'Skipped: %d', 'pneupex-supplier-importer' ), $skipped ); ?></li>
				</ul>
				<?php if ( $synced === 0 && $errors === 0 && $skipped > 0 ) : ?>
					<p class="description">
						<?php esc_html_e( 'All products were skipped (likely missing modulario_id). Check logs for details.', 'pneupex-supplier-importer' ); ?>
					</p>
				<?php elseif ( $synced === 0 && $errors === 0 && $skipped === 0 ) : ?>
					<p class="description">
						<?php esc_html_e( 'No products were processed. Check that product IDs are valid and have modulario_id set.', 'pneupex-supplier-importer' ); ?>
					</p>
				<?php endif; ?>
			</div>
			<?php
		}

		?>
		<div class="wrap">
			<h1><?php esc_html_e( 'Supplier Importer', 'pneupex-supplier-importer' ); ?></h1>
			
			<div class="card">
				<h2><?php esc_html_e( 'Run Import', 'pneupex-supplier-importer' ); ?></h2>
				<form method="post" action="<?php echo esc_url( admin_url( 'admin-post.php' ) ); ?>">
					<?php wp_nonce_field( 'pneupex_run_import', 'pneupex_import_nonce' ); ?>
					<input type="hidden" name="action" value="pneupex_run_import">
					
					<table class="form-table">
						<tr>
							<th scope="row">
								<label for="supplier"><?php esc_html_e( 'Supplier', 'pneupex-supplier-importer' ); ?></label>
							</th>
							<td>
								<select name="supplier" id="supplier" required>
									<option value=""><?php esc_html_e( '-- Select Supplier --', 'pneupex-supplier-importer' ); ?></option>
									<?php foreach ( $suppliers as $supplier ) : ?>
										<option value="<?php echo esc_attr( $supplier ); ?>">
											<?php echo esc_html( $supplier ); ?>
										</option>
									<?php endforeach; ?>
								</select>
							</td>
						</tr>
						<tr>
							<th scope="row">
								<label for="row_limit"><?php esc_html_e( 'Row Limit (Testing)', 'pneupex-supplier-importer' ); ?></label>
							</th>
							<td>
								<input type="number" name="row_limit" id="row_limit" min="1" step="1" value="" placeholder="<?php esc_attr_e( 'Leave empty for all rows', 'pneupex-supplier-importer' ); ?>" />
								<p class="description">
									<?php esc_html_e( 'Optional: Limit number of CSV rows to process (useful for testing). Leave empty to process all rows.', 'pneupex-supplier-importer' ); ?>
								</p>
							</td>
						</tr>
					</table>
					
					<?php submit_button( __( 'Run Import', 'pneupex-supplier-importer' ) ); ?>
				</form>
			</div>

			<div class="card">
				<h2><?php esc_html_e( 'Test Modulario Sync', 'pneupex-supplier-importer' ); ?></h2>
				<p><?php esc_html_e( 'Test the Modulario sync functionality with specific product IDs or products from a supplier import.', 'pneupex-supplier-importer' ); ?></p>
				<form method="post" action="<?php echo esc_url( admin_url( 'admin-post.php' ) ); ?>">
					<?php wp_nonce_field( 'pneupex_test_modulario_sync', 'pneupex_test_sync_nonce' ); ?>
					<input type="hidden" name="action" value="pneupex_test_modulario_sync">
					
					<table class="form-table">
						<tr>
							<th scope="row">
								<label for="test_supplier"><?php esc_html_e( 'Supplier (optional)', 'pneupex-supplier-importer' ); ?></label>
							</th>
							<td>
								<select name="test_supplier" id="test_supplier">
									<option value=""><?php esc_html_e( '-- Use Product IDs below --', 'pneupex-supplier-importer' ); ?></option>
									<?php foreach ( $mapper->get_all_suppliers() as $supplier ) : ?>
										<option value="<?php echo esc_attr( $supplier ); ?>"><?php echo esc_html( $supplier ); ?></option>
									<?php endforeach; ?>
								</select>
								<p class="description">
									<?php esc_html_e( 'Select a supplier to test with products from its last import, or leave empty and provide product IDs below.', 'pneupex-supplier-importer' ); ?>
								</p>
							</td>
						</tr>
						<tr>
							<th scope="row">
								<label for="test_product_ids"><?php esc_html_e( 'Product IDs (optional)', 'pneupex-supplier-importer' ); ?></label>
							</th>
							<td>
								<input type="text" name="test_product_ids" id="test_product_ids" class="regular-text" 
										placeholder="<?php esc_attr_e( '123, 456, 789', 'pneupex-supplier-importer' ); ?>">
								<p class="description">
									<?php esc_html_e( 'Comma-separated list of product IDs to test. Leave empty if using supplier option above.', 'pneupex-supplier-importer' ); ?>
								</p>
							</td>
						</tr>
					</table>
					
					<?php submit_button( __( 'Test Modulario Sync', 'pneupex-supplier-importer' ), 'secondary' ); ?>
				</form>
			</div>
			
			<div class="card">
				<h2><?php esc_html_e( 'Cron Schedule', 'pneupex-supplier-importer' ); ?></h2>
				<form method="post" action="<?php echo esc_url( admin_url( 'admin-post.php' ) ); ?>">
					<?php wp_nonce_field( 'pneupex_save_schedule', 'pneupex_schedule_nonce' ); ?>
					<input type="hidden" name="action" value="pneupex_save_schedule">
					
					<?php
					$current_schedule = get_option( 'pneupex_supplier_import_schedule', '' );
					$next_run         = wp_next_scheduled( 'pneupex_supplier_import_cron' );
					?>
					
					<table class="form-table">
						<tr>
							<th scope="row">
								<label for="schedule"><?php esc_html_e( 'Import Schedule', 'pneupex-supplier-importer' ); ?></label>
							</th>
							<td>
								<select name="schedule" id="schedule">
									<option value=""><?php esc_html_e( '-- Disabled --', 'pneupex-supplier-importer' ); ?></option>
									<option value="hourly" <?php selected( $current_schedule, 'hourly' ); ?>><?php esc_html_e( 'Hourly', 'pneupex-supplier-importer' ); ?></option>
									<option value="twicedaily" <?php selected( $current_schedule, 'twicedaily' ); ?>><?php esc_html_e( 'Twice Daily', 'pneupex-supplier-importer' ); ?></option>
									<option value="daily" <?php selected( $current_schedule, 'daily' ); ?>><?php esc_html_e( 'Daily', 'pneupex-supplier-importer' ); ?></option>
								</select>
								<p class="description">
									<?php esc_html_e( 'Select how often to automatically import all suppliers. Leave disabled to run imports manually only.', 'pneupex-supplier-importer' ); ?>
								</p>
							</td>
						</tr>
						<tr>
							<th scope="row"><?php esc_html_e( 'Status', 'pneupex-supplier-importer' ); ?></th>
							<td>
								<?php if ( $next_run ) : ?>
									<p>
										<strong><?php esc_html_e( 'Next run:', 'pneupex-supplier-importer' ); ?></strong>
										<?php echo esc_html( date_i18n( get_option( 'date_format' ) . ' ' . get_option( 'time_format' ), $next_run ) ); ?>
									</p>
								<?php else : ?>
									<p><em><?php esc_html_e( 'Cron is not scheduled.', 'pneupex-supplier-importer' ); ?></em></p>
								<?php endif; ?>
							</td>
						</tr>
					</table>
					
					<?php submit_button( __( 'Save Schedule', 'pneupex-supplier-importer' ) ); ?>
				</form>
			</div>
			
			<div class="card">
				<h2><?php esc_html_e( 'Supplier ID Mapping', 'pneupex-supplier-importer' ); ?></h2>
				<p><?php esc_html_e( 'Current mapping (can be updated once real IDs are available):', 'pneupex-supplier-importer' ); ?></p>
				<table class="widefat">
					<thead>
						<tr>
							<th><?php esc_html_e( 'Supplier Name', 'pneupex-supplier-importer' ); ?></th>
							<th><?php esc_html_e( 'ID', 'pneupex-supplier-importer' ); ?></th>
						</tr>
					</thead>
					<tbody>
						<?php
						$mapping = $mapper->get_mapping();
						foreach ( $mapping as $name => $id ) :
							?>
							<tr>
								<td><?php echo esc_html( $name ); ?></td>
								<td><?php echo esc_html( $id ); ?></td>
							</tr>
						<?php endforeach; ?>
					</tbody>
				</table>
			</div>
		</div>
		<?php
	}

	/**
	 * Handle manual import from admin
	 */
	public function handle_manual_import(): void {
		if ( ! current_user_can( 'manage_woocommerce' ) ) {
			wp_die( __( 'You do not have permission to run imports.', 'pneupex-supplier-importer' ) );
		}

		if ( ! isset( $_POST['pneupex_import_nonce'] ) ||
			! wp_verify_nonce( $_POST['pneupex_import_nonce'], 'pneupex_run_import' ) ) {
			wp_die( __( 'Security check failed.', 'pneupex-supplier-importer' ) );
		}

		$supplier_name = isset( $_POST['supplier'] ) ? sanitize_text_field( wp_unslash( $_POST['supplier'] ) ) : '';

		if ( empty( $supplier_name ) ) {
			wp_die( esc_html__( 'Please select a supplier.', 'pneupex-supplier-importer' ) );
		}

		// Get optional row limit for testing
		$row_limit = null;
		if ( isset( $_POST['row_limit'] ) && ! empty( $_POST['row_limit'] ) ) {
			$row_limit_raw = sanitize_text_field( wp_unslash( $_POST['row_limit'] ) );
			$row_limit     = max( 1, absint( $row_limit_raw ) );
		}

		$importer = $this->get_importer_instance( $supplier_name );
		if ( ! $importer ) {
			wp_die( sprintf( __( 'Importer class not found for supplier: %s', 'pneupex-supplier-importer' ), $supplier_name ) );
		}

		// Set flags to disable expensive hooks during import (same as cron)
		if ( ! defined( 'PNEUPEX_SUPPLIER_IMPORTING' ) ) {
			define( 'PNEUPEX_SUPPLIER_IMPORTING', true );
		}
		if ( ! defined( 'CEOD_IMPORTING' ) ) {
			define( 'CEOD_IMPORTING', true );
		}

		// Pass row limit to import method if specified
		$stats = $importer->import( $row_limit );

		// Sync updated products to Modulario after import (before closing log)
		$modulario_stats            = $importer->sync_updated_products_to_modulario();
		$stats['modulario_synced']  = $modulario_stats['synced'];
		$stats['modulario_errors']  = $modulario_stats['errors'];
		$stats['modulario_skipped'] = $modulario_stats['skipped'];

		// Close log file after sync completes (import() leaves it open for sync logging)
		$importer->close_log();

		// Clear flags after import completes
		if ( defined( 'PNEUPEX_SUPPLIER_IMPORTING' ) ) {
			// Constants can't be undefined, but we can note that import is complete
		}

		wp_redirect(
			add_query_arg(
				array(
					'page'             => 'pneupex-supplier-importer',
					'imported'         => '1',
					'processed'        => $stats['processed'],
					'updated'          => $stats['updated'] ?? 0,
					'errors'           => $stats['errors'],
					'modulario_synced' => $modulario_stats['synced'],
					'modulario_errors' => $modulario_stats['errors'],
				),
				admin_url( 'admin.php' )
			)
		);
		exit;
	}

	/**
	 * Handle test Modulario sync from admin
	 */
	public function handle_test_modulario_sync(): void {
		if ( ! current_user_can( 'manage_woocommerce' ) ) {
			wp_die( __( 'You do not have permission to test Modulario sync.', 'pneupex-supplier-importer' ) );
		}

		if ( ! isset( $_POST['pneupex_test_sync_nonce'] ) ||
			! wp_verify_nonce( $_POST['pneupex_test_sync_nonce'], 'pneupex_test_modulario_sync' ) ) {
			wp_die( __( 'Security check failed.', 'pneupex-supplier-importer' ) );
		}

		$supplier          = isset( $_POST['test_supplier'] ) ? sanitize_text_field( wp_unslash( $_POST['test_supplier'] ) ) : '';
		$product_ids_input = isset( $_POST['test_product_ids'] ) ? sanitize_text_field( wp_unslash( $_POST['test_product_ids'] ) ) : '';

		$product_ids = array();
		if ( ! empty( $product_ids_input ) ) {
			$product_ids = array_map( 'intval', array_filter( array_map( 'trim', explode( ',', $product_ids_input ) ) ) );
		}

		if ( ! empty( $supplier ) ) {
			$importer = $this->get_importer_instance( $supplier );
			if ( ! $importer ) {
				wp_die( sprintf( __( 'Importer class not found for supplier: %s', 'pneupex-supplier-importer' ), $supplier ) );
			}
			$stats = $importer->test_modulario_sync( $product_ids );
		} elseif ( ! empty( $product_ids ) ) {
			// Get any importer instance to use the test method
			$mapper    = new Pneupex_Supplier_ID_Mapper();
			$suppliers = $mapper->get_all_suppliers();
			if ( empty( $suppliers ) ) {
				wp_die( __( 'No suppliers found.', 'pneupex-supplier-importer' ) );
			}
			$importer = $this->get_importer_instance( $suppliers[0] );
			if ( ! $importer ) {
				wp_die( __( 'Could not create importer instance.', 'pneupex-supplier-importer' ) );
			}
			$stats = $importer->test_modulario_sync( $product_ids );
		} else {
			wp_die( __( 'Please provide either a supplier name or product IDs.', 'pneupex-supplier-importer' ) );
		}

		wp_redirect(
			add_query_arg(
				array(
					'page'      => 'pneupex-supplier-importer',
					'test_sync' => '1',
					'synced'    => $stats['synced'],
					'errors'    => $stats['errors'],
					'skipped'   => $stats['skipped'],
				),
				admin_url( 'admin.php' )
			)
		);
		exit;
	}

	/**
	 * Handle save schedule from admin
	 */
	public function handle_save_schedule(): void {
		if ( ! current_user_can( 'manage_woocommerce' ) ) {
			wp_die( __( 'You do not have permission to manage schedules.', 'pneupex-supplier-importer' ) );
		}

		if ( ! isset( $_POST['pneupex_schedule_nonce'] ) ||
			! wp_verify_nonce( $_POST['pneupex_schedule_nonce'], 'pneupex_save_schedule' ) ) {
			wp_die( __( 'Security check failed.', 'pneupex-supplier-importer' ) );
		}

		$schedule = isset( $_POST['schedule'] ) ? sanitize_text_field( wp_unslash( $_POST['schedule'] ) ) : '';

		// Valid schedule options
		$valid_schedules = array( 'hourly', 'twicedaily', 'daily' );
		if ( ! empty( $schedule ) && ! in_array( $schedule, $valid_schedules, true ) ) {
			wp_die( __( 'Invalid schedule option.', 'pneupex-supplier-importer' ) );
		}

		// Save schedule option
		if ( empty( $schedule ) ) {
			delete_option( 'pneupex_supplier_import_schedule' );
		} else {
			update_option( 'pneupex_supplier_import_schedule', $schedule );
		}

		// Clear transient to force re-check on next admin page load
		delete_transient( 'pneupex_cron_check' );

		// Unschedule existing cron
		$timestamp = wp_next_scheduled( 'pneupex_supplier_import_cron' );
		if ( $timestamp ) {
			wp_unschedule_event( $timestamp, 'pneupex_supplier_import_cron' );
		}

		// Schedule new cron if schedule is set
		if ( ! empty( $schedule ) ) {
			wp_schedule_event( time(), $schedule, 'pneupex_supplier_import_cron' );
		}

		wp_redirect(
			add_query_arg(
				array(
					'page'           => 'pneupex-supplier-importer',
					'schedule_saved' => '1',
				),
				admin_url( 'admin.php' )
			)
		);
		exit;
	}

	/**
	 * Run cron import (all suppliers or specific)
	 * Optimized for long-running processes with memory management and delays
	 */
	public function run_cron_import( string $supplier_name = '' ): void {
		// Safety check: ensure plugin is still active
		if ( ! class_exists( 'Pneupex_Supplier_Importer' ) ) {
			// Plugin was deactivated, unschedule this cron
			$timestamp = wp_next_scheduled( 'pneupex_supplier_import_cron' );
			if ( $timestamp ) {
				wp_unschedule_event( $timestamp, 'pneupex_supplier_import_cron' );
			}
			return;
		}

		// Extend execution time for cron jobs
		if ( function_exists( 'set_time_limit' ) ) {
			@set_time_limit( 3600 ); // 1 hour for all suppliers
		}
		if ( function_exists( 'ini_set' ) ) {
			@ini_set( 'max_execution_time', '3600' );
			@ini_set( 'memory_limit', '512M' ); // Increase memory limit
		}

		$mapper    = new Pneupex_Supplier_ID_Mapper();
		$suppliers = empty( $supplier_name ) ? $mapper->get_all_suppliers() : array( $supplier_name );

		$total_suppliers = count( $suppliers );
		$supplier_index  = 0;

		// Collect all updated product IDs across ALL suppliers to avoid duplicate syncs
		// This prevents the same product from being synced multiple times if it appears in multiple suppliers
		$all_updated_product_ids = array();

		foreach ( $suppliers as $supplier ) {
			++$supplier_index;

			error_log( "[PNEUPEX_CRON] Processing supplier {$supplier_index}/{$total_suppliers}: {$supplier}" );

			try {
				$importer = $this->get_importer_instance( $supplier );
				if ( $importer ) {
					$importer->import();

					// Collect updated product IDs from this supplier (don't sync yet)
					$supplier_updated_ids = $importer->get_updated_product_ids();
					if ( ! empty( $supplier_updated_ids ) ) {
						$all_updated_product_ids = array_merge( $all_updated_product_ids, $supplier_updated_ids );
						error_log(
							sprintf(
								'[PNEUPEX_CRON] Supplier %s updated %d products',
								$supplier,
								count( $supplier_updated_ids )
							)
						);
					}
				}
			} catch ( \Throwable $e ) {
				error_log( "[PNEUPEX_CRON] Error processing supplier {$supplier}: " . $e->getMessage() );
			}

			// Clear memory between suppliers
			$this->clear_memory_after_supplier();

			// Add delay between suppliers to prevent server overload
			// Only delay if not the last supplier
			if ( $supplier_index < $total_suppliers ) {
				$delay_seconds = apply_filters( 'pneupex_supplier_import_delay', 5 ); // Default 5 seconds
				if ( $delay_seconds > 0 ) {
					sleep( $delay_seconds );
				}
			}
		}

		// Sync all updated products to Modulario ONCE at the end (with global deduplication)
		// This prevents the same product from being synced multiple times if it appears in multiple suppliers
		if ( ! empty( $all_updated_product_ids ) ) {
			// Global deduplication across all suppliers
			$unique_product_ids = array_unique( $all_updated_product_ids );
			$duplicate_count    = count( $all_updated_product_ids ) - count( $unique_product_ids );

			error_log(
				sprintf(
					'[PNEUPEX_CRON] Global Modulario sync: %d unique products (removed %d duplicates across suppliers)',
					count( $unique_product_ids ),
					$duplicate_count
				)
			);

			// Use the first importer instance to sync (they all have the same sync method)
			// Pass product IDs directly to avoid reflection complexity
			if ( ! empty( $suppliers ) ) {
				try {
					$first_importer = $this->get_importer_instance( $suppliers[0] );
					if ( $first_importer ) {
						// Perform global sync with deduplicated product IDs
						$modulario_stats = $first_importer->sync_updated_products_to_modulario( $unique_product_ids );

						error_log(
							sprintf(
								'[PNEUPEX_CRON] Global Modulario sync completed: %d synced, %d errors, %d skipped',
								$modulario_stats['synced'],
								$modulario_stats['errors'],
								$modulario_stats['skipped']
							)
						);
					}
				} catch ( \Throwable $e ) {
					error_log( '[PNEUPEX_CRON] Error during global Modulario sync: ' . $e->getMessage() );
				}
			}
		} else {
			error_log( '[PNEUPEX_CRON] No products were updated across all suppliers. Skipping Modulario sync.' );
		}

		error_log( '[PNEUPEX_CRON] Completed processing all suppliers' );
	}

	/**
	 * Clear memory after processing a supplier
	 * Uses targeted clearing instead of full cache flush (more efficient)
	 */
	private function clear_memory_after_supplier(): void {
		// Clear ACF cache if available
		if ( function_exists( 'acf_get_store' ) ) {
			acf_get_store( 'values' )->reset();
		}

		// Force garbage collection
		if ( function_exists( 'gc_collect_cycles' ) ) {
			gc_collect_cycles();
		}

		// Note: We don't use wp_cache_flush() here as it clears the entire cache
		// Individual products are cleared via clean_post_cache() in the processing methods
	}

	/**
	 * Get importer instance by supplier name
	 */
	private function get_importer_instance( string $supplier_name ): ?Pneupex_Supplier_Importer_Base {
		// Handle special cases first
		// If name ends with .cz or .nl (lowercase with dot), remove the dot and suffix
		if ( preg_match( '/\.(cz|nl)$/i', $supplier_name ) ) {
			$normalized = preg_replace( '/\.(cz|nl)$/i', '', $supplier_name );
		} else {
			// For names ending with CZ or NL (uppercase, no dot), keep them as is
			// For other cases, convert to lowercase and process
			$normalized = $supplier_name;
		}

		// Convert to lowercase, but preserve CZ/NL at the end if they're uppercase
		if ( preg_match( '/(CZ|NL)$/', $normalized ) ) {
			// Name already has CZ/NL uppercase, keep it
			$base       = substr( $normalized, 0, -2 );
			$suffix     = substr( $normalized, -2 );
			$normalized = strtolower( $base ) . $suffix;
		} else {
			$normalized = strtolower( $normalized );
		}

		// Remove dots, dashes, and underscores
		$normalized = str_replace( array( '.', '-', '_' ), '', $normalized );

		// Convert to class name format: capitalize words
		$class_name = 'Pneupex_' . str_replace( ' ', '_', ucwords( str_replace( '_', ' ', $normalized ) ) ) . '_Importer';

		if ( class_exists( $class_name ) ) {
			return new $class_name();
		}

		return null;
	}

	/**
	 * Schedule cron if enabled
	 * Uses transient cache to avoid running on every page load
	 */
	public function maybe_schedule_cron(): void {
		// Safety check: only run if WordPress is fully loaded
		if ( ! function_exists( 'get_option' ) || ! function_exists( 'wp_next_scheduled' ) ) {
			return;
		}

		// Use transient to cache check - only run once per hour
		$cache_key = 'pneupex_cron_check';
		if ( get_transient( $cache_key ) ) {
			return;
		}

		try {
			$schedule = get_option( 'pneupex_supplier_import_schedule', '' );

			if ( empty( $schedule ) ) {
				// Unschedule if disabled
				$timestamp = wp_next_scheduled( 'pneupex_supplier_import_cron' );
				if ( $timestamp ) {
					wp_unschedule_event( $timestamp, 'pneupex_supplier_import_cron' );
				}
				set_transient( $cache_key, true, 3600 ); // Cache for 1 hour
				return;
			}

			// Schedule if not already scheduled
			if ( ! wp_next_scheduled( 'pneupex_supplier_import_cron' ) ) {
				wp_schedule_event( time(), $schedule, 'pneupex_supplier_import_cron' );
			}

			set_transient( $cache_key, true, 3600 ); // Cache for 1 hour
		} catch ( \Throwable $e ) {
			// Log error but don't break page loads
			error_log( '[PNEUPEX_SUPPLIER_IMPORTER] Cron scheduling error: ' . $e->getMessage() );
			// Still set transient to avoid repeated failures
			set_transient( $cache_key, true, 300 ); // Cache for 5 minutes on error
		}
	}

	/**
	 * Schedule cleanup cron
	 * Uses transient cache to avoid running on every page load
	 */
	public function maybe_schedule_cleanup(): void {
		// Safety check: only run if WordPress is fully loaded
		if ( ! function_exists( 'wp_next_scheduled' ) ) {
			return;
		}

		// Use transient to cache check - only run once per hour
		$cache_key = 'pneupex_cleanup_check';
		if ( get_transient( $cache_key ) ) {
			return;
		}

		try {
			if ( ! wp_next_scheduled( 'pneupex_supplier_cleanup' ) ) {
				wp_schedule_event( time(), 'daily', 'pneupex_supplier_cleanup' );
			}

			set_transient( $cache_key, true, 3600 ); // Cache for 1 hour
		} catch ( \Throwable $e ) {
			// Log error but don't break page loads
			error_log( '[PNEUPEX_SUPPLIER_IMPORTER] Cleanup scheduling error: ' . $e->getMessage() );
			// Still set transient to avoid repeated failures
			set_transient( $cache_key, true, 300 ); // Cache for 5 minutes on error
		}
	}

	/**
	 * Run cleanup tasks
	 */
	public function run_cleanup(): void {
		// Safety check: ensure plugin is still active
		if ( ! class_exists( 'Pneupex_Supplier_Importer' ) ) {
			// Plugin was deactivated, unschedule this cron
			$timestamp = wp_next_scheduled( 'pneupex_supplier_cleanup' );
			if ( $timestamp ) {
				wp_unschedule_event( $timestamp, 'pneupex_supplier_cleanup' );
			}
			return;
		}

		require_once PNEUPEX_SUPPLIER_IMPORTER_PATH . 'includes/class-cleanup-manager.php';
		Pneupex_Cleanup_Manager::run_cleanup();
	}

	/**
	 * Cleanup old temp files on shutdown (safety net)
	 */
	public function cleanup_old_temp_files(): void {
		// Only run occasionally to avoid performance impact
		$last_cleanup = get_transient( 'pneupex_last_temp_cleanup' );
		if ( $last_cleanup && ( time() - $last_cleanup ) < 3600 ) {
			return; // Run max once per hour
		}

		try {
			// Check if class exists (might already be loaded)
			if ( ! class_exists( 'Pneupex_Cleanup_Manager' ) ) {
				$cleanup_file = PNEUPEX_SUPPLIER_IMPORTER_PATH . 'includes/class-cleanup-manager.php';
				if ( file_exists( $cleanup_file ) ) {
					require_once $cleanup_file;
				} else {
					return; // File doesn't exist, skip cleanup
				}
			}

			Pneupex_Cleanup_Manager::cleanup_temp_files();
			set_transient( 'pneupex_last_temp_cleanup', time(), 3600 );
		} catch ( \Throwable $e ) {
			// Silently fail on shutdown to avoid breaking the page
			error_log( '[PNEUPEX_CLEANUP] Shutdown cleanup error: ' . $e->getMessage() );
		}
	}
}

// Initialize plugin after WordPress and dependencies are loaded
add_action(
	'plugins_loaded',
	function () {
		// Allow plugin to be disabled via wp-config.php constant (safe before filters exist)
		if ( defined( 'PNEUPEX_SUPPLIER_IMPORTER_DISABLED' ) && PNEUPEX_SUPPLIER_IMPORTER_DISABLED ) {
			return;
		}

		// Allow plugin to be disabled via filter
		if ( ! apply_filters( 'pneupex_supplier_importer_enabled', true ) ) {
			return;
		}

		try {
			// Check for required dependencies
			if ( ! class_exists( 'WooCommerce' ) ) {
				add_action(
					'admin_notices',
					function () {
						echo '<div class="notice notice-error"><p>';
						echo esc_html__( 'Pneupex Supplier Importer requires WooCommerce to be installed and activated.', 'pneupex-supplier-importer' );
						echo '</p></div>';
					}
				);
				return;
			}

			if ( ! function_exists( 'get_field' ) ) {
				add_action(
					'admin_notices',
					function () {
						echo '<div class="notice notice-error"><p>';
						echo esc_html__( 'Pneupex Supplier Importer requires Advanced Custom Fields (ACF) to be installed and activated.', 'pneupex-supplier-importer' );
						echo '</p></div>';
					}
				);
				return;
			}

			Pneupex_Supplier_Importer::get_instance();
		} catch ( \Throwable $e ) {
			// Log fatal errors that might not be caught by WordPress
			error_log( '[PNEUPEX_SUPPLIER_IMPORTER] Fatal error: ' . $e->getMessage() . ' in ' . $e->getFile() . ':' . $e->getLine() );
			error_log( '[PNEUPEX_SUPPLIER_IMPORTER] Stack trace: ' . $e->getTraceAsString() );

			// Try to display error if in admin
			if ( is_admin() ) {
				add_action(
					'admin_notices',
					function () use ( $e ) {
						echo '<div class="notice notice-error"><p>';
						echo '<strong>Pneupex Supplier Importer Error:</strong> ' . esc_html( $e->getMessage() );
						echo '<br><small>File: ' . esc_html( $e->getFile() ) . ':' . esc_html( (string) $e->getLine() ) . '</small>';
						echo '</p></div>';
					}
				);
			}
		}
	},
	20
);

// Catch errors during plugin file loading (only log, don't interfere)
// Only register if we're actually initializing the plugin AND plugin is active
// This check happens at file load time, so we need to check the constant here
if ( ! ( defined( 'PNEUPEX_SUPPLIER_IMPORTER_DISABLED' ) && PNEUPEX_SUPPLIER_IMPORTER_DISABLED ) ) {
	// Check if plugin is actually active before registering shutdown function
	add_action(
		'plugins_loaded',
		function () {
			// Double-check constant and if plugin class was actually initialized
			if ( ! ( defined( 'PNEUPEX_SUPPLIER_IMPORTER_DISABLED' ) && PNEUPEX_SUPPLIER_IMPORTER_DISABLED )
			&& class_exists( 'Pneupex_Supplier_Importer' ) ) {
				register_shutdown_function(
					function () {
						$error = error_get_last();
						if ( $error && in_array( $error['type'], array( E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR ) ) ) {
								// Check if error is from our plugin
							if ( strpos( $error['file'], 'pneupex-supplier-importer' ) !== false ) {
								error_log( '[PNEUPEX_SUPPLIER_IMPORTER] Shutdown error: ' . $error['message'] . ' in ' . $error['file'] . ':' . $error['line'] );
							}
						}
					}
				);
			}
		},
		30
	); // Run after plugin initialization (priority 30 > 20)
}

// Register activation hook to schedule cron jobs
register_activation_hook(
	__FILE__,
	function () {
		// Schedule cleanup cron on activation
		if ( ! wp_next_scheduled( 'pneupex_supplier_cleanup' ) ) {
			wp_schedule_event( time(), 'daily', 'pneupex_supplier_cleanup' );
		}
	}
);

// Register deactivation hook to clean up cron jobs
register_deactivation_hook(
	__FILE__,
	function () {
		// Clear all scheduled cron jobs
		$cron_timestamp = wp_next_scheduled( 'pneupex_supplier_import_cron' );
		if ( $cron_timestamp ) {
			wp_unschedule_event( $cron_timestamp, 'pneupex_supplier_import_cron' );
		}

		$cleanup_timestamp = wp_next_scheduled( 'pneupex_supplier_cleanup' );
		if ( $cleanup_timestamp ) {
			wp_unschedule_event( $cleanup_timestamp, 'pneupex_supplier_cleanup' );
		}

		// Clear transients
		delete_transient( 'pneupex_cron_check' );
		delete_transient( 'pneupex_cleanup_check' );
		delete_transient( 'pneupex_last_temp_cleanup' );
	}
);

// NOTE: Do not add a default filter here; it prevents disabling via wp-config.php (filters don't exist there).
