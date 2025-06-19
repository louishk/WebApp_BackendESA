<?php
/**
 * Enhanced Configuration Class for RapidStor Descriptor Manager
 * Now supports .env file loading with fallbacks
 */
class Config
{
    private static $loaded = false;
    private static $envVars = [];

    // Default values (fallbacks)
    const DEFAULT_API_BASE_URL = 'https://api.redboxstorage.hk';
    const DEFAULT_DEBUG_MODE = false;
    const DEFAULT_MEMORY_LIMIT = '256M';
    const DEFAULT_MAX_EXECUTION_TIME = 120;
    const DEFAULT_BATCH_SIZE = 10;
    const DEFAULT_REQUEST_TIMEOUT = 30;
    const DEFAULT_LOCATION = 'L004';

    const LOCATIONS = [
        'L004' => 'Location 004',
        'L005' => 'Location 005',
        'L006' => 'Location 006',
        'L007' => 'Location 007',
        'L008' => 'Location 008',
        'L009' => 'Location 009',
        'L010' => 'Location 010'
    ];

    // Default descriptor template for new descriptors
    const DEFAULT_DESCRIPTOR = [
        'descriptions' => [''],
        'spacerEnabled' => false,
        'criteria' => [
            'include' => [
                'sizes' => [],
                'keywords' => [],
                'floors' => [],
                'features' => ['climate' => null, 'inside' => null, 'alarm' => null, 'power' => null],
                'prices' => []
            ],
            'exclude' => [
                'sizes' => [],
                'keywords' => [],
                'floors' => [],
                'features' => ['climate' => null, 'inside' => null, 'alarm' => null, 'power' => null],
                'prices' => []
            ]
        ],
        'deals' => [],
        'tags' => [],
        'upgradesTo' => [],
        'slides' => [],
        'picture' => 'https://ik.imagekit.io/bytcx9plm/150x150.png',
        'highlight' => ['use' => false, 'colour' => '#ffffff', 'flag' => false],
        'defaultInsuranceCoverage' => '6723aaef41549342379e4dfd',
        'sCorpCode' => 'CNCK'
    ];

    /**
     * Load environment variables from .env file
     */
    private static function loadEnv()
    {
        if (self::$loaded) {
            return;
        }

        self::$loaded = true;

        // Look for .env file in current directory or parent directories
        $envFile = self::findEnvFile();

        if ($envFile && file_exists($envFile)) {
            self::parseEnvFile($envFile);
        }

        // Apply PHP settings from env vars
        self::applyPhpSettings();
    }

    /**
     * Find .env file in current or parent directories
     */
    private static function findEnvFile()
    {
        $currentDir = __DIR__;
        $maxLevels = 3; // Don't go too far up

        for ($i = 0; $i < $maxLevels; $i++) {
            $envPath = $currentDir . DIRECTORY_SEPARATOR . '.env';
            if (file_exists($envPath)) {
                return $envPath;
            }

            $parentDir = dirname($currentDir);
            if ($parentDir === $currentDir) {
                break; // Reached root directory
            }
            $currentDir = $parentDir;
        }

        return null;
    }

    /**
     * Parse .env file and load variables
     */
    private static function parseEnvFile($filePath)
    {
        try {
            $lines = file($filePath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);

            foreach ($lines as $line) {
                // Skip comments and empty lines
                $line = trim($line);
                if (empty($line) || $line[0] === '#') {
                    continue;
                }

                // Parse KEY=VALUE format
                if (strpos($line, '=') !== false) {
                    list($key, $value) = explode('=', $line, 2);

                    $key = trim($key);
                    $value = trim($value);

                    // Remove quotes if present
                    if (($value[0] === '"' && $value[-1] === '"') ||
                        ($value[0] === "'" && $value[-1] === "'")) {
                        $value = substr($value, 1, -1);
                    }

                    // Store in our array and set as environment variable
                    self::$envVars[$key] = $value;
                    if (!array_key_exists($key, $_ENV)) {
                        $_ENV[$key] = $value;
                        putenv("$key=$value");
                    }
                }
            }
        } catch (Exception $e) {
            error_log("Failed to load .env file: " . $e->getMessage());
        }
    }

    /**
     * Apply PHP settings from environment variables
     */
    private static function applyPhpSettings()
    {
        $memoryLimit = self::get('MEMORY_LIMIT');
        if ($memoryLimit) {
            ini_set('memory_limit', $memoryLimit);
        }

        $maxExecutionTime = self::get('MAX_EXECUTION_TIME');
        if ($maxExecutionTime) {
            ini_set('max_execution_time', $maxExecutionTime);
        }

        // Set error reporting based on debug mode
        if (self::isDebugMode()) {
            error_reporting(E_ALL);
            ini_set('display_errors', 1);
        } else {
            error_reporting(E_ERROR | E_WARNING);
            ini_set('display_errors', 0);
        }
    }

    /**
     * Get environment variable with fallback to default
     */
    public static function get($key, $default = null)
    {
        self::loadEnv();

        // First check our loaded env vars
        if (isset(self::$envVars[$key])) {
            return self::convertValue(self::$envVars[$key]);
        }

        // Then check system environment
        $value = getenv($key);
        if ($value !== false) {
            return self::convertValue($value);
        }

        // Check $_ENV superglobal
        if (isset($_ENV[$key])) {
            return self::convertValue($_ENV[$key]);
        }

        return $default;
    }

    /**
     * Convert string values to appropriate types
     */
    private static function convertValue($value)
    {
        if ($value === '') {
            return null;
        }

        // Convert boolean strings
        $lowercaseValue = strtolower($value);
        if (in_array($lowercaseValue, ['true', 'false'])) {
            return $lowercaseValue === 'true';
        }

        // Convert numeric strings
        if (is_numeric($value)) {
            return strpos($value, '.') !== false ? (float)$value : (int)$value;
        }

        return $value;
    }

    /**
     * Get API base URL
     */
    public static function getApiBaseUrl()
    {
        return self::get('API_BASE_URL', self::DEFAULT_API_BASE_URL);
    }

    /**
     * Get JWT token
     */
    public static function getJwtToken()
    {
        return self::get('JWT_TOKEN');
    }

    /**
     * Check if debug mode is enabled
     */
    public static function isDebugMode()
    {
        return self::get('DEBUG_MODE', self::DEFAULT_DEBUG_MODE);
    }

    /**
     * Get batch size for operations
     */
    public static function getBatchSize()
    {
        return self::get('BATCH_SIZE', self::DEFAULT_BATCH_SIZE);
    }

    /**
     * Get request timeout
     */
    public static function getRequestTimeout()
    {
        return self::get('REQUEST_TIMEOUT', self::DEFAULT_REQUEST_TIMEOUT);
    }

    /**
     * Get default location
     */
    public static function getDefaultLocation()
    {
        $location = self::get('DEFAULT_LOCATION', self::DEFAULT_LOCATION);
        return self::isValidLocation($location) ? $location : self::DEFAULT_LOCATION;
    }

    /**
     * Get location name
     */
    public static function getLocationName($code)
    {
        return self::LOCATIONS[$code] ?? "Unknown Location ($code)";
    }

    /**
     * Check if location is valid
     */
    public static function isValidLocation($code)
    {
        return array_key_exists($code, self::LOCATIONS);
    }

    /**
     * Get all loaded environment variables (for debugging)
     */
    public static function getAllEnvVars()
    {
        self::loadEnv();
        return self::$envVars;
    }

    /**
     * Check if .env file was found and loaded
     */
    public static function isEnvFileLoaded()
    {
        self::loadEnv();
        return !empty(self::$envVars);
    }

    /**
     * Get .env file path if found
     */
    public static function getEnvFilePath()
    {
        return self::findEnvFile();
    }

    /**
     * Create a sample .env file
     */
    public static function createSampleEnvFile($path = '.env.example')
    {
        $content = <<<ENV
# RapidStor Descriptor Manager Configuration

# API Configuration
API_BASE_URL=https://api.redboxstorage.hk
JWT_TOKEN=

# Application Settings
DEBUG_MODE=false
MEMORY_LIMIT=256M
MAX_EXECUTION_TIME=120
BATCH_SIZE=10
REQUEST_TIMEOUT=30

# Optional Settings
DEFAULT_LOCATION=L005

ENV;

        return file_put_contents($path, $content) !== false;
    }
}

// Auto-load environment on include
Config::get('_AUTOLOAD_TRIGGER_');
?>