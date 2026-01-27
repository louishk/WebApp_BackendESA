<?php
/**
 * Health Check Script
 * Verifies database connection and basic configuration
 *
 * Usage: php healthcheck.php
 * Or access via web: /healthcheck.php
 */

header('Content-Type: application/json');

$results = [
    'timestamp' => date('c'),
    'checks' => [],
    'overall' => 'healthy'
];

// Check 1: PHP Version
$phpVersion = phpversion();
$results['checks']['php_version'] = [
    'status' => version_compare($phpVersion, '8.0.0', '>=') ? 'pass' : 'warn',
    'value' => $phpVersion,
    'required' => '8.0+'
];

// Check 2: Required PHP Extensions
$requiredExtensions = ['pgsql', 'pdo_pgsql', 'json', 'curl', 'mbstring'];
foreach ($requiredExtensions as $ext) {
    $loaded = extension_loaded($ext);
    $results['checks']["ext_$ext"] = [
        'status' => $loaded ? 'pass' : 'fail',
        'value' => $loaded ? 'loaded' : 'missing'
    ];
    if (!$loaded) {
        $results['overall'] = 'unhealthy';
    }
}

// Check 3: Environment file
$envFile = __DIR__ . '/.env';
$results['checks']['env_file'] = [
    'status' => file_exists($envFile) ? 'pass' : 'fail',
    'value' => file_exists($envFile) ? 'exists' : 'missing'
];

// Load config if env exists
if (file_exists($envFile)) {
    try {
        require_once __DIR__ . '/vendor/autoload.php';
        $dotenv = Dotenv\Dotenv::createImmutable(__DIR__);
        $dotenv->load();

        // Check 4: Database Configuration
        $dbHost = $_ENV['DB_HOST'] ?? null;
        $dbName = $_ENV['DB_NAME'] ?? null;
        $dbUser = $_ENV['DB_USERNAME'] ?? null;

        $results['checks']['db_config'] = [
            'status' => ($dbHost && $dbName && $dbUser) ? 'pass' : 'fail',
            'value' => [
                'host' => $dbHost ? 'configured' : 'missing',
                'database' => $dbName ? 'configured' : 'missing',
                'username' => $dbUser ? 'configured' : 'missing'
            ]
        ];

        // Check 5: Database Connection
        if ($dbHost && $dbName && $dbUser) {
            try {
                $dbPort = $_ENV['DB_PORT'] ?? '5432';
                $dbPass = $_ENV['DB_PASSWORD'] ?? '';
                $sslMode = $_ENV['DB_SSLMODE'] ?? 'require';

                $dsn = "pgsql:host={$dbHost};port={$dbPort};dbname={$dbName};sslmode={$sslMode}";
                $pdo = new PDO($dsn, $dbUser, $dbPass, [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_TIMEOUT => 5
                ]);

                // Test query
                $stmt = $pdo->query("SELECT 1");
                $stmt->fetch();

                $results['checks']['db_connection'] = [
                    'status' => 'pass',
                    'value' => 'connected'
                ];

                // Check tables exist
                $tables = ['users', 'pages', 'schema_markups'];
                foreach ($tables as $table) {
                    try {
                        $stmt = $pdo->query("SELECT 1 FROM {$table} LIMIT 1");
                        $results['checks']["table_{$table}"] = [
                            'status' => 'pass',
                            'value' => 'exists'
                        ];
                    } catch (PDOException $e) {
                        $results['checks']["table_{$table}"] = [
                            'status' => 'warn',
                            'value' => 'missing or empty'
                        ];
                    }
                }

            } catch (PDOException $e) {
                $results['checks']['db_connection'] = [
                    'status' => 'fail',
                    'value' => 'failed',
                    'error' => $e->getMessage()
                ];
                $results['overall'] = 'unhealthy';
            }
        }

        // Check 6: OAuth Configuration
        $oauthClientId = $_ENV['MS_OAUTH_CLIENT_ID'] ?? null;
        $oauthTenant = $_ENV['MS_OAUTH_TENANT'] ?? null;
        $results['checks']['oauth_config'] = [
            'status' => ($oauthClientId && $oauthTenant) ? 'pass' : 'warn',
            'value' => [
                'client_id' => $oauthClientId ? 'configured' : 'missing',
                'tenant' => $oauthTenant ? 'configured' : 'missing'
            ]
        ];

        // Check 7: JWT Configuration
        $jwtSecret = $_ENV['JWT_SECRET'] ?? null;
        $results['checks']['jwt_config'] = [
            'status' => ($jwtSecret && strlen($jwtSecret) >= 32) ? 'pass' : 'warn',
            'value' => $jwtSecret ? (strlen($jwtSecret) >= 32 ? 'configured' : 'too short') : 'missing'
        ];

        // Check 8: Scheduler API
        $schedulerUrl = $_ENV['SCHEDULER_API_URL'] ?? 'http://localhost:5000';
        $ch = curl_init($schedulerUrl . '/health');
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT => 3,
            CURLOPT_CONNECTTIMEOUT => 2
        ]);
        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        $results['checks']['scheduler_api'] = [
            'status' => ($httpCode === 200) ? 'pass' : 'warn',
            'value' => ($httpCode === 200) ? 'reachable' : 'unreachable',
            'url' => $schedulerUrl
        ];

    } catch (Exception $e) {
        $results['checks']['config_load'] = [
            'status' => 'fail',
            'value' => 'error',
            'error' => $e->getMessage()
        ];
        $results['overall'] = 'unhealthy';
    }
}

// Determine overall status
foreach ($results['checks'] as $check) {
    if ($check['status'] === 'fail') {
        $results['overall'] = 'unhealthy';
        break;
    }
}

// Set HTTP status code
if ($results['overall'] === 'unhealthy') {
    http_response_code(503);
}

echo json_encode($results, JSON_PRETTY_PRINT);
