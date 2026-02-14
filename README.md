# Pneupex Supplier Importer

WordPress plugin for importing external supplier stock data into WooCommerce products via ACF repeater fields and synchronise them with Modulario system.

## Features

- Imports stock data from multiple suppliers (Michelin, Continental, Bridgestone, Goodyear, IHLE, Handlopex, Alcar, Asteel, DobrePneu.cz, GPD, Vredestein, Vandenban CZ, Vandenban NL)
- Updates ACF repeater field `external_warehouse` on products
- Supports FTP and SFTP file downloads
- Price calculation with configurable coefficients (markups)
- Automatic synchronization with Modulario system after imports
- Custom logging system
- Optional WordPress cron scheduling for automated imports
- Automatic cleanup of temporary files and old logs
- Simple admin UI for testing

## Installation

1. Place plugin in `wp-content/plugins/pneupex-supplier-importer/`
2. Activate plugin in WordPress admin
3. Define FTP/SFTP credentials as constants in `wp-config.php` (see Configuration)
4. Install Composer dependencies: `composer install` (required for SFTP support via phpseclib)

## Configuration

### FTP/SFTP Credentials

Add constants to `wp-config.php` for each supplier. All suppliers use FTP except Bridgestone and Alcar (SFTP):

```php
// Michelin (FTP)
define( 'PNEUPEX_MICHELIN_FTP_HOST', 'ftp.pexstore.sk' );
define( 'PNEUPEX_MICHELIN_FTP_USER', 'username' );
define( 'PNEUPEX_MICHELIN_FTP_PASS', 'password' );
define( 'PNEUPEX_MICHELIN_FTP_DIR', 'michelin' );

// Continental (FTP)
define( 'PNEUPEX_CONTINENTAL_FTP_HOST', 'ftp.pexstore.sk' );
define( 'PNEUPEX_CONTINENTAL_FTP_USER', 'username' );
define( 'PNEUPEX_CONTINENTAL_FTP_PASS', 'password' );
define( 'PNEUPEX_CONTINENTAL_FTP_DIR', 'continental' );

// Add more suppliers as needed...
```

**Note:** For SFTP, the plugin uses phpseclib library (installed via Composer). Ensure `composer install` has been run to install dependencies.

### Supplier ID Mapping

Supplier names are mapped to numeric IDs (1-x). Default mapping is in `Pneupex_Supplier_ID_Mapper`. Update via filter:

```php
add_filter( 'pneupex_supplier_id_mapping', function( $mapping ) {
    $mapping['Michelin'] = 1;
    $mapping['Goodyear'] = 2;
    // ... update as needed
    return $mapping;
} );
```

### Coefficients

Price coefficients (markups) are stored in the `eshop_suppliers_coefficients` database table. The plugin automatically looks up coefficients based on:
- Supplier name
- Product type (optional, wildcard if empty)
- Product brand (optional, wildcard if empty)

Coefficients are cached for performance and can be managed directly in the database table. The lookup prioritizes the most specific match (supplier + type + brand > supplier + type > supplier + brand > supplier only).

## Usage

### Admin UI

1. Go to **Supplier Import** in WordPress admin
2. Select supplier from dropdown
3. Click **Run Import**
4. After import completes, products are automatically synced to Modulario system

### Modulario Synchronization

The plugin automatically synchronizes updated products to the Modulario system after each import completes. This ensures that external warehouse data (stock quantities and prices) is kept in sync between WooCommerce and Modulario.

**How it works:**
- After an import finishes, all products that were updated are automatically synced to Modulario
- Sync is performed in batches (default: 100 products per batch) to prevent API overload
- Rate limiting is enforced (default: 2 seconds between API calls, max 30 requests per minute)
- Only products with a `modulario_id` meta field are synced
- Products are deduplicated across multiple suppliers to avoid duplicate syncs

**Testing Modulario Sync:**
- Use the "Test Modulario Sync" section in the admin UI
- You can test with specific product IDs or products from a supplier's last import
- Results show how many products were synced, skipped (missing modulario_id), or had errors

**Configuration:**
You can customize the batch size and rate limiting via WordPress filters:

```php
// Change batch size (default: 100)
add_filter( 'pneupex_modulario_sync_batch_size', function() {
    return 50; // Process 50 products per batch
} );

// Change rate limit delay (default: 2 seconds = 2000000 microseconds)
add_filter( 'pneupex_modulario_rate_limit_delay', function() {
    return 3000000; // 3 seconds between calls
} );

// Change max requests per minute (default: 30)
add_filter( 'pneupex_modulario_rate_limit_per_minute', function() {
    return 20; // Max 20 requests per minute
} );
```

### Cron Scheduling

The plugin uses two types of cron jobs:

#### 1. Import Cron (Optional)
Automatically imports supplier data at scheduled intervals. **Disabled by default** - must be enabled manually:

```php
// Enable hourly imports
update_option( 'pneupex_supplier_import_schedule', 'hourly' );

// Other options: 'twicedaily', 'daily'
// To disable: update_option( 'pneupex_supplier_import_schedule', '' );
```

**Note:** The import cron only runs if the schedule option is set. If not set, no automatic imports occur.

#### 2. Cleanup Cron (Automatic)
Automatically scheduled on plugin activation. Runs **daily** to clean up:
- Temporary files older than 1 hour (from FTP/SFTP downloads)
- Log files older than 7 days

This cron cannot be disabled and is essential for maintaining disk space. It automatically unschedules itself if the plugin is deactivated.

#### Disabling Plugin via wp-config.php

You can disable the entire plugin without deactivating it:

```php
define( 'PNEUPEX_SUPPLIER_IMPORTER_DISABLED', true );
```

This prevents the plugin from loading and executing any code, including cron jobs.

## Architecture

- **Main Plugin Class**: `Pneupex_Supplier_Importer` - Plugin initialization, admin UI, cron management
- **Base Class**: `Pneupex_Supplier_Importer_Base` - Common functionality for all suppliers
- **Supplier Classes**: Extend base class (e.g., `Pneupex_Michelin_Importer`, `Pneupex_Continental_Importer`)
- **Logger**: `Pneupex_Supplier_Logger` - Custom logging to files
- **Mapper**: `Pneupex_Supplier_ID_Mapper` - Supplier name to ID mapping
- **Coefficient Manager**: `Pneupex_Coefficient_Manager` - Price markup management
- **FTP Handler**: `Pneupex_FTP_Handler` - FTP/SFTP file download utilities (supports phpseclib)
- **Cleanup Manager**: `Pneupex_Cleanup_Manager` - Automatic cleanup of temp files and logs

## ACF Field Structure

The plugin updates the `external_warehouse` repeater field with:
- `external_warehouse_id` - Supplier ID (number 1-x)
- `external_warehouse_product_quantity` - Stock quantity
- `external_warehouse_product_price` - Calculated price

## Logging

Logs are stored in `wp-content/uploads/pneupex-supplier-logs/` with format:
`{supplier_name}_{timestamp}.log`

Each log file contains detailed information about the import process, including:
- File download status
- Products processed, updated, skipped, and errors
- Modulario sync results
- Performance metrics and warnings

## Development

### WordPress Coding Standards Setup

This plugin follows WordPress Coding Standards. To set up and use PHPCS:

**1. Install Composer dependencies:**
```bash
composer install
```

This will install:
- `wp-coding-standards/wpcs` - WordPress Coding Standards
- `phpcompatibility/phpcompatibility-wp` - PHP compatibility checks
- `squizlabs/php_codesniffer` - PHPCS tool

**2. Run code quality checks:**
```bash
# Check code against WordPress standards
composer run phpcs

# Auto-fix issues where possible
composer run phpcbf
```

**3. Configuration:**
The `phpcs.xml` file in the root directory configures:
- WordPress Coding Standards as the base ruleset
- PHP 8.0+ compatibility checks
- Exclusions for vendor directory
- Custom rules (allows short array syntax, long lines, etc.)

**4. Understanding the output:**
- PHPCS will report any code style violations
- Errors must be fixed manually
- Warnings can often be auto-fixed with `phpcbf`
- The report shows file, line number, and rule violated

## Notes

- Products are matched by SKU (EAN from CSV = SKU in WooCommerce)
- Price calculation: `base_price * coefficient`
- Coefficients are stored in `eshop_suppliers_coefficients` database table
- Coefficients are looked up by: supplier + product type + brand (most specific match wins)
- Updates existing rows in ACF repeater (doesn't delete all, updates in place)
- Plugin automatically cleans up cron jobs on deactivation
- All cron scheduling uses transients to minimize database queries
- Database transactions are used when supported (InnoDB engine)
- API rate limiting is enforced for Modulario sync calls
- Modulario sync runs automatically after each import completes