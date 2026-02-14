<?php
declare(strict_types=1);

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

/**
 * Handles FTP and SFTP connections for downloading supplier files
 */
class Pneupex_FTP_Handler {

	private ?string $temp_dir = null;

	public function __construct() {
		// Don't initialize temp_dir here - lazy load it when needed
	}

	/**
	 * Get temp directory (lazy initialization)
	 */
	private function get_temp_dir_internal(): string {
		if ( $this->temp_dir !== null ) {
			return $this->temp_dir;
		}

		if ( ! function_exists( 'wp_upload_dir' ) ) {
			$this->temp_dir = WP_CONTENT_DIR . '/uploads/pneupex-temp/';
		} else {
			$upload_dir = wp_upload_dir();
			if ( ! $upload_dir || isset( $upload_dir['error'] ) ) {
				$this->temp_dir = WP_CONTENT_DIR . '/uploads/pneupex-temp/';
			} else {
				$this->temp_dir = trailingslashit( $upload_dir['basedir'] ) . 'pneupex-temp/';
			}
		}

		if ( ! file_exists( $this->temp_dir ) ) {
			if ( function_exists( 'wp_mkdir_p' ) ) {
				wp_mkdir_p( $this->temp_dir );
			} else {
				mkdir( $this->temp_dir, 0755, true );
			}
		}

		return $this->temp_dir;
	}

	/**
	 * Download file via FTP
	 */
	public function download_via_ftp( string $host, string $username, string $password, string $remote_path, ?string $base_dir = null ): ?string {
		$connection = ftp_connect( $host );

		if ( ! $connection ) {
			return null;
		}

		if ( ! ftp_login( $connection, $username, $password ) ) {
			ftp_close( $connection );
			return null;
		}

		if ( $base_dir ) {
			ftp_chdir( $connection, $base_dir );
		}

		// Get file list and find latest file if needed
		$file_list = ftp_nlist( $connection, '.' );
		if ( $file_list === false ) {
			ftp_close( $connection );
			return null;
		}

		// If remote_path contains wildcard or is directory, find latest file
		$remote_file = $remote_path;
		if ( strpos( $remote_path, '*' ) !== false || is_dir( $remote_path ) ) {
			$files = array_filter(
				$file_list,
				function ( $file ) use ( $remote_path ) {
					return fnmatch( $remote_path, $file );
				}
			);

			if ( empty( $files ) ) {
				ftp_close( $connection );
				return null;
			}

			// Get latest file by modification time
			$latest_file = '';
			$latest_time = 0;
			foreach ( $files as $file ) {
				$time = ftp_mdtm( $connection, $file );
				if ( $time > $latest_time ) {
					$latest_time = $time;
					$latest_file = $file;
				}
			}
			$remote_file = $latest_file;
		}

		// Download file
		$local_file = $this->get_temp_dir_internal() . basename( $remote_file ) . '_' . time() . '.csv';
		$result     = ftp_get( $connection, $local_file, $remote_file, FTP_BINARY );

		ftp_close( $connection );

		return $result ? $local_file : null;
	}

	/**
	 * Download file via SFTP using phpseclib (pure PHP, no extension needed)
	 */
	public function download_via_sftp( string $host, int $port, string $username, string $password, string $remote_path ): ?string {
		// Check if phpseclib is already loaded (e.g. by wp-all-import-pro) - use it to avoid conflicts
		$sftp_class = null;
		if ( class_exists( '\phpseclib3\Net\SFTP' ) ) {
			$sftp_class = '\phpseclib3\Net\SFTP';
		} elseif ( class_exists( '\phpseclib\Net\SFTP' ) ) {
			$sftp_class = '\phpseclib\Net\SFTP';
		}

		// If not loaded, try to load it
		if ( ! $sftp_class ) {
			$phpseclib_loaded = $this->load_phpseclib();

			if ( ! $phpseclib_loaded ) {
				error_log( 'Pneupex SFTP: phpseclib library not found. Cannot download via SFTP.' );
				return null;
			}

			// Check again after loading
			if ( class_exists( '\phpseclib3\Net\SFTP' ) ) {
				$sftp_class = '\phpseclib3\Net\SFTP';
			} elseif ( class_exists( '\phpseclib\Net\SFTP' ) ) {
				$sftp_class = '\phpseclib\Net\SFTP';
			}
		}

		if ( ! $sftp_class ) {
			error_log( 'Pneupex SFTP: phpseclib SFTP class not available after loading attempt.' );
			return null;
		}

		try {
			// Use the detected class (already loaded or just loaded)
			error_log( "Pneupex SFTP: Attempting connection to {$host}:{$port}" );

			// Note: phpseclib connects lazily, but we can set timeout before connection
			// Create SFTP connection - connection happens lazily on first operation
			$sftp = new $sftp_class( $host, $port );

			// Set timeout for connection (30 seconds) - must be set before connection attempt
			if ( method_exists( $sftp, 'setTimeout' ) ) {
				$sftp->setTimeout( 30 );
				error_log( 'Pneupex SFTP: Connection timeout set to 30 seconds' );
			}

			error_log( 'Pneupex SFTP: SFTP object created, attempting connection...' );

			// Authenticate
			error_log( "Pneupex SFTP: Attempting login with username: {$username}" );
			$login_result = $sftp->login( $username, $password );

			if ( ! $login_result ) {
				$last_error = 'Unknown error';
				if ( method_exists( $sftp, 'getLastError' ) ) {
					$last_error = $sftp->getLastError();
				} elseif ( method_exists( $sftp, 'getErrors' ) ) {
					$errors     = $sftp->getErrors();
					$last_error = ! empty( $errors ) ? implode( ', ', $errors ) : 'Unknown error';
				} elseif ( method_exists( $sftp, 'getServerIdentification' ) ) {
					// Sometimes we can get more info
					$server_id = $sftp->getServerIdentification();
					error_log( 'Pneupex SFTP: Server identification: ' . ( $server_id ?: 'unknown' ) );
				}
				error_log( "Pneupex SFTP: Login failed. Error: {$last_error}" );
				return null;
			}

			error_log( 'Pneupex SFTP: Login successful' );

			// Ensure remote_path is properly formatted
			// If path doesn't start with /, assume it's in root directory
			if ( ! empty( $remote_path ) && $remote_path[0] !== '/' ) {
				$remote_path = '/' . $remote_path;
			}

			error_log( "Pneupex SFTP: Attempting to download file: {$remote_path}" );

			// Check if file exists first (if method available)
			if ( method_exists( $sftp, 'file_exists' ) ) {
				if ( ! $sftp->file_exists( $remote_path ) ) {
					error_log( "Pneupex SFTP: File does not exist on server: {$remote_path}" );
					// Try to list directory to see what's available
					$dir = dirname( $remote_path );
					if ( $dir === '.' || $dir === '/' ) {
						$dir = '/';
					}
					if ( method_exists( $sftp, 'nlist' ) ) {
						$files = $sftp->nlist( $dir );
						error_log( "Pneupex SFTP: Files in directory {$dir}: " . ( $files ? implode( ', ', $files ) : 'none' ) );
					}
					return null;
				}
			}

			// Download file
			$file_content = $sftp->get( $remote_path );

			if ( $file_content === false ) {
				$last_error = method_exists( $sftp, 'getLastError' ) ? $sftp->getLastError() : 'Unknown error';
				error_log( "Pneupex SFTP: Failed to download file: {$remote_path}. Error: {$last_error}" );
				return null;
			}

			if ( $file_content === null || $file_content === '' ) {
				error_log( "Pneupex SFTP: Downloaded file is empty: {$remote_path}" );
				return null;
			}

			// Save to local file
			$local_file = $this->get_temp_dir_internal() . basename( $remote_path ) . '_' . time() . '.csv';
			error_log( "Pneupex SFTP: Saving downloaded file to: {$local_file}" );
			$result = file_put_contents( $local_file, $file_content );

			if ( $result === false || ! file_exists( $local_file ) || filesize( $local_file ) === 0 ) {
				error_log( 'Pneupex SFTP: Failed to save file locally. Result: ' . var_export( $result, true ) );
				if ( file_exists( $local_file ) ) {
					@unlink( $local_file );
				}
				return null;
			}

			$file_size = filesize( $local_file );
			error_log( "Pneupex SFTP: Successfully downloaded file: {$remote_path} ({$file_size} bytes)" );

			return $local_file;
		} catch ( \Exception $e ) {
			$error_msg   = $e->getMessage();
			$error_class = get_class( $e );

			// Provide more helpful error messages for common connection issues
			if ( strpos( $error_msg, 'Connection refused' ) !== false || strpos( $error_msg, 'Error 111' ) !== false ) {
				error_log( "Pneupex SFTP: Connection refused to {$host}:{$port}. Possible causes:" );
				error_log( "  - Firewall blocking port {$port}" );
				error_log( '  - Server not accessible from hosting environment' );
				error_log( '  - Server requires VPN or whitelisted IP addresses' );
				error_log( "  - Incorrect port number (current: {$port})" );
				error_log( '  - Server is down or not accepting connections' );
			} elseif ( strpos( $error_msg, 'Connection timed out' ) !== false ) {
				error_log( "Pneupex SFTP: Connection timed out to {$host}:{$port}. Server may be slow or unreachable." );
			} elseif ( strpos( $error_msg, 'Unable to connect' ) !== false ) {
				error_log( "Pneupex SFTP: Unable to connect to {$host}:{$port}. Check network connectivity and server status." );
			}

			error_log( "Pneupex SFTP: Exception ({$error_class}): " . $error_msg . ' in ' . $e->getFile() . ':' . $e->getLine() );

			// Log stack trace for debugging (first 5 lines to avoid huge logs)
			$trace       = $e->getTraceAsString();
			$trace_lines = explode( "\n", $trace );
			error_log( 'Pneupex SFTP: Stack trace (first 5 lines):' );
			foreach ( array_slice( $trace_lines, 0, 5 ) as $line ) {
				error_log( '  ' . trim( $line ) );
			}

			return null;
		} catch ( \Throwable $e ) {
			$error_msg   = $e->getMessage();
			$error_class = get_class( $e );
			error_log( "Pneupex SFTP: Throwable ({$error_class}): " . $error_msg . ' in ' . $e->getFile() . ':' . $e->getLine() );
			return null;
		}
	}

	/**
	 * Load phpseclib library
	 * Supports both Composer autoloader and manual installation
	 * Also checks if phpseclib is already loaded by another plugin (e.g. wp-all-import-pro)
	 */
	private function load_phpseclib(): bool {
		// First check if phpseclib is already loaded (e.g. by wp-all-import-pro)
		if ( class_exists( '\phpseclib3\Net\SFTP' ) || class_exists( '\phpseclib\Net\SFTP' ) ) {
			return true;
		}

		// Try autoloader first (if installed via Composer)
		$autoloader_paths = array(
			PNEUPEX_SUPPLIER_IMPORTER_PATH . 'vendor/autoload.php',
			WP_PLUGIN_DIR . '/../vendor/autoload.php',
		);

		foreach ( $autoloader_paths as $autoloader ) {
			if ( file_exists( $autoloader ) ) {
				require_once $autoloader;
				// Check if classes are available after autoloader
				if ( class_exists( '\phpseclib3\Net\SFTP' ) || class_exists( '\phpseclib\Net\SFTP' ) ) {
					return true;
				}
			}
		}

		// Try to use phpseclib from wp-all-import-pro if it exists
		$wpai_phpseclib = WP_PLUGIN_DIR . '/wp-all-import-pro/vendor/phpseclib/phpseclib/phpseclib/';
		if ( file_exists( $wpai_phpseclib . 'Net/SFTP.php' ) ) {
			// Don't load it manually - let wp-all-import-pro's autoloader handle it
			// Just check if class exists after a moment
			if ( class_exists( '\phpseclib3\Net\SFTP' ) || class_exists( '\phpseclib\Net\SFTP' ) ) {
				return true;
			}
		}

		// Try manual loading (if downloaded manually) - only if not already loaded
		$phpseclib_paths = array(
			PNEUPEX_SUPPLIER_IMPORTER_PATH . 'vendor/phpseclib/phpseclib/phpseclib/',
			PNEUPEX_SUPPLIER_IMPORTER_PATH . 'vendor/phpseclib/phpseclib/',
		);

		foreach ( $phpseclib_paths as $base_path ) {
			if ( file_exists( $base_path . 'Net/SFTP.php' ) ) {
				// Only load if class doesn't exist yet (avoid conflicts)
				if ( ! class_exists( '\phpseclib3\Net\SFTP' ) && ! class_exists( '\phpseclib\Net\SFTP' ) ) {
					// Register simple autoloader for phpseclib
					spl_autoload_register(
						function ( $class ) use ( $base_path ) {
							// Convert namespace to path
							$class       = ltrim( $class, '\\' );
							$class_parts = explode( '\\', $class );

							// Handle phpseclib3 and phpseclib namespaces
							if ( $class_parts[0] === 'phpseclib3' || $class_parts[0] === 'phpseclib' ) {
									array_shift( $class_parts ); // Remove namespace prefix
									$file = $base_path . implode( '/', $class_parts ) . '.php';

								if ( file_exists( $file ) ) {
									require_once $file;
									return true;
								}
							}

							return false;
						},
						true,
						true
					); // prepend=true, throw=true

					// Try to load SFTP class
					if ( file_exists( $base_path . 'Net/SFTP.php' ) ) {
						require_once $base_path . 'Net/SFTP.php';
						if ( class_exists( '\phpseclib3\Net\SFTP' ) || class_exists( '\phpseclib\Net\SFTP' ) ) {
							return true;
						}
					}
				} else {
					// Class already exists, use it
					return true;
				}
			}
		}

		return false;
	}

	/**
	 * Download file via FTP URL (file_get_contents)
	 * Note: This method has limitations. For better reliability, use download_via_ftp() instead.
	 */
	public function download_via_url( string $url ): ?string {
		error_log( "Pneupex FTP URL: Starting download from: {$url}" );

		// Check if allow_url_fopen is enabled (required for FTP URLs)
		if ( ! ini_get( 'allow_url_fopen' ) ) {
			error_log( 'Pneupex FTP URL: allow_url_fopen is disabled. Cannot download via FTP URL.' );
			return null;
		}

		error_log( 'Pneupex FTP URL: allow_url_fopen is enabled' );

		// Parse URL to extract components for better error handling
		$url_parts = parse_url( $url );
		if ( ! $url_parts ) {
			error_log( "Pneupex FTP URL: Failed to parse URL: {$url}" );
			return null;
		}

		if ( $url_parts['scheme'] !== 'ftp' ) {
			error_log( "Pneupex FTP URL: URL scheme is not 'ftp': " . ( $url_parts['scheme'] ?? 'unknown' ) );
			return null;
		}

		$host = $url_parts['host'] ?? 'unknown';
		$port = $url_parts['port'] ?? 21;
		$path = $url_parts['path'] ?? '/';
		$user = $url_parts['user'] ?? 'anonymous';

		error_log( "Pneupex FTP URL: Parsed URL - Host: {$host}, Port: {$port}, Path: {$path}, User: {$user}" );

		// Create FTP context with proper settings
		$context = stream_context_create(
			array(
				'ftp' => array(
					'timeout'   => 300,
					'overwrite' => true,
				),
			)
		);

		error_log( 'Pneupex FTP URL: Created stream context with 300 second timeout' );

		// Suppress warnings to capture error details
		$error = null;
		set_error_handler(
			function ( $errno, $errstr ) use ( &$error ) {
				$error = $errstr;
				return true;
			},
			E_WARNING | E_NOTICE
		);

		error_log( 'Pneupex FTP URL: Attempting to download file via file_get_contents...' );
		$start_time   = microtime( true );
		$content      = @file_get_contents( $url, false, $context );
		$elapsed_time = round( microtime( true ) - $start_time, 2 );

		restore_error_handler();

		if ( $content === false ) {
			$error_msg = $error ?: 'Unknown error';
			error_log( "Pneupex FTP URL: Download failed after {$elapsed_time} seconds. Error: {$error_msg}" );
			error_log( 'Pneupex FTP URL: Possible causes:' );
			error_log( '  - FTP server is unreachable or down' );
			error_log( '  - Incorrect credentials in URL' );
			error_log( "  - File does not exist on server: {$path}" );
			error_log( "  - Network/firewall blocking connection to {$host}:{$port}" );
			error_log( '  - Server requires passive mode (file_get_contents may not support this)' );

			// Try alternative: use FTP functions instead
			return null;
		}

		$content_size = strlen( $content );
		error_log( "Pneupex FTP URL: Successfully downloaded {$content_size} bytes in {$elapsed_time} seconds" );

		$local_file = $this->get_temp_dir_internal() . basename( $url_parts['path'] ) . '_' . time() . '.csv';
		error_log( "Pneupex FTP URL: Saving downloaded content to: {$local_file}" );

		$result = file_put_contents( $local_file, $content );

		if ( $result === false ) {
			error_log( "Pneupex FTP URL: Failed to save file to local filesystem: {$local_file}" );
			return null;
		}

		$saved_size = file_exists( $local_file ) ? filesize( $local_file ) : 0;
		error_log( "Pneupex FTP URL: File saved successfully. Local file size: {$saved_size} bytes" );

		if ( $saved_size === 0 ) {
			error_log( 'Pneupex FTP URL: WARNING - Saved file is empty!' );
			@unlink( $local_file );
			return null;
		}

		if ( $saved_size !== $content_size ) {
			error_log( "Pneupex FTP URL: WARNING - File size mismatch. Downloaded: {$content_size} bytes, Saved: {$saved_size} bytes" );
		}

		return $local_file;
	}

	/**
	 * Clean up temp file
	 */
	public function cleanup( string $file_path ): void {
		if ( file_exists( $file_path ) ) {
			unlink( $file_path );
		}
	}

	/**
	 * Get temp directory
	 */
	public function get_temp_dir(): string {
		return $this->get_temp_dir_internal();
	}

	/**
	 * Enable passive mode on FTP connection
	 */
	public function set_passive_mode( $connection, bool $passive = true ): bool {
		return ftp_pasv( $connection, $passive );
	}

	/**
	 * Connect to FTP server and login
	 *
	 * @param string $host FTP host
	 * @param string $username FTP username
	 * @param string $password FTP password
	 * @param int    $port FTP port (default: 21)
	 * @param int    $timeout Connection timeout in seconds (default: 30)
	 * @return resource|false FTP connection resource or false on failure
	 */
	public function connect_and_login( string $host, string $username, string $password, int $port = 21, int $timeout = 30 ) {
		$connection = @ftp_connect( $host, $port, $timeout );

		if ( ! $connection ) {
			error_log( "Pneupex FTP: Failed to connect to FTP server: {$host}:{$port}" );
			return false;
		}

		if ( ! @ftp_login( $connection, $username, $password ) ) {
			error_log( "Pneupex FTP: Failed to login to FTP server: {$host}" );
			ftp_close( $connection );
			return false;
		}

		return $connection;
	}

	/**
	 * Configure FTP connection mode (passive/active)
	 * Tries passive first, falls back to active if passive fails
	 *
	 * @param resource $connection FTP connection resource
	 * @param bool     $prefer_passive Whether to prefer passive mode (default: true)
	 * @return bool True if mode was set successfully
	 */
	public function configure_connection_mode( $connection, bool $prefer_passive = true ): bool {
		if ( $prefer_passive ) {
			$pasv_result = @ftp_pasv( $connection, true );
			if ( $pasv_result ) {
				return true;
			}

			// Fallback to active mode
			$active_result = @ftp_pasv( $connection, false );
			if ( $active_result ) {
				error_log( 'Pneupex FTP: Passive mode failed, using active mode as fallback' );
				return true;
			}

			error_log( 'Pneupex FTP: Failed to set both passive and active mode' );
			return false;
		} else {
			return @ftp_pasv( $connection, false );
		}
	}

	/**
	 * Change to a directory on FTP server
	 *
	 * @param resource $connection FTP connection resource
	 * @param string   $directory Directory path
	 * @return bool True on success, false on failure
	 */
	public function change_directory( $connection, string $directory ): bool {
		if ( ! @ftp_chdir( $connection, $directory ) ) {
			$current_dir = @ftp_pwd( $connection );
			error_log( "Pneupex FTP: Failed to change to directory: {$directory}. Current directory: " . ( $current_dir ?: 'unknown' ) );
			return false;
		}
		return true;
	}

	/**
	 * Get file list from FTP server
	 * Tries multiple methods: nlist first, then rawlist with parsing
	 *
	 * @param resource $connection FTP connection resource
	 * @param string   $directory Directory to list (default: '.')
	 * @return array|false Array of filenames or false on failure
	 */
	public function get_file_list( $connection, string $directory = '.' ) {
		// Method 1: Try nlist (simplest, returns just filenames)
		$file_list = @ftp_nlist( $connection, $directory );
		if ( $file_list !== false ) {
			// Filter out . and .. entries
			return array_filter(
				$file_list,
				function ( $file ) {
					$basename = basename( $file );
					return $basename !== '.' && $basename !== '..';
				}
			);
		}

		// Method 2: Try rawlist (more detailed, need to parse)
		$raw_list = @ftp_rawlist( $connection, $directory );
		if ( $raw_list === false || empty( $raw_list ) ) {
			error_log( "Pneupex FTP: Both nlist and rawlist failed for directory: {$directory}" );
			return false;
		}

		// Parse rawlist format to extract filenames
		$file_list = array();
		foreach ( $raw_list as $line ) {
			if ( empty( trim( $line ) ) ) {
				continue;
			}

			// Try to extract filename (usually last field after date/time)
			$parts = preg_split( '/\s+/', trim( $line ), 9 );
			if ( count( $parts ) >= 9 ) {
				$filename = $parts[8];
				if ( $filename !== '.' && $filename !== '..' ) {
					$file_list[] = $filename;
				}
			} elseif ( count( $parts ) > 0 ) {
				$filename = end( $parts );
				if ( $filename !== '.' && $filename !== '..' && ! empty( $filename ) ) {
					$file_list[] = $filename;
				}
			}
		}

		return ! empty( $file_list ) ? $file_list : false;
	}

	/**
	 * Get latest file from FTP server by modification time
	 *
	 * @param resource $connection FTP connection resource
	 * @param array    $file_list Array of filenames
	 * @return string|false Filename of latest file or false on failure
	 */
	public function get_latest_file( $connection, array $file_list ) {
		if ( empty( $file_list ) ) {
			return false;
		}

		$latest_file = '';
		$latest_time = 0;

		foreach ( $file_list as $file ) {
			$file_name = basename( $file );
			$time      = @ftp_mdtm( $connection, $file_name );
			if ( $time > $latest_time ) {
				$latest_time = $time;
				$latest_file = $file_name;
			}
		}

		// Fallback: use last file in list if mdtm failed
		if ( empty( $latest_file ) ) {
			$latest_file = basename( end( $file_list ) );
		}

		return $latest_file;
	}

	/**
	 * Download file from FTP server
	 *
	 * @param resource    $connection FTP connection resource
	 * @param string      $remote_file Remote filename
	 * @param string|null $local_file Local file path (if null, generates temp path)
	 * @return string|false Local file path on success, false on failure
	 */
	public function download_file( $connection, string $remote_file, ?string $local_file = null ) {
		if ( $local_file === null ) {
			$local_file = $this->get_temp_dir_internal() . basename( $remote_file ) . '_' . time() . '.csv';
		}

		// Remove existing file if it exists
		if ( file_exists( $local_file ) ) {
			@unlink( $local_file );
		}

		$result = @ftp_get( $connection, $local_file, $remote_file, FTP_BINARY );

		if ( ! $result ) {
			$error     = error_get_last();
			$error_msg = $error ? $error['message'] : 'Unknown error';
			error_log( "Pneupex FTP: Failed to download file: {$remote_file}. Error: {$error_msg}" );
			return false;
		}

		// Verify file was downloaded
		if ( ! file_exists( $local_file ) ) {
			error_log( "Pneupex FTP: Downloaded file does not exist: {$local_file}" );
			return false;
		}

		$file_size = filesize( $local_file );
		if ( $file_size === 0 ) {
			error_log( "Pneupex FTP: Downloaded file is empty: {$local_file}" );
			@unlink( $local_file );
			return false;
		}

		return $local_file;
	}

	/**
	 * Complete FTP workflow: connect, login, configure, change directory, get latest file, download
	 *
	 * @param string      $host FTP host
	 * @param string      $username FTP username
	 * @param string      $password FTP password
	 * @param string      $base_dir Base directory to change to
	 * @param string|null $remote_file Specific file to download (if null, finds latest)
	 * @param int         $port FTP port (default: 21)
	 * @return string|false Local file path on success, false on failure
	 */
	public function download_via_ftp_complete( string $host, string $username, string $password, string $base_dir, ?string $remote_file = null, int $port = 21 ): ?string {
		$connection = $this->connect_and_login( $host, $username, $password, $port );
		if ( ! $connection ) {
			return null;
		}

		// Configure connection mode
		if ( ! $this->configure_connection_mode( $connection, true ) ) {
			ftp_close( $connection );
			return null;
		}

		// Change to base directory
		if ( ! empty( $base_dir ) && ! $this->change_directory( $connection, $base_dir ) ) {
			ftp_close( $connection );
			return null;
		}

		// Get file to download
		if ( $remote_file === null ) {
			$file_list = $this->get_file_list( $connection );
			if ( $file_list === false || empty( $file_list ) ) {
				error_log( "Pneupex FTP: No files found in directory: {$base_dir}" );
				ftp_close( $connection );
				return null;
			}
			$remote_file = $this->get_latest_file( $connection, $file_list );
			if ( $remote_file === false ) {
				ftp_close( $connection );
				return null;
			}
		}

		// Download file
		$local_file = $this->download_file( $connection, $remote_file );

		// Close connection
		ftp_close( $connection );

		return $local_file;
	}
}
