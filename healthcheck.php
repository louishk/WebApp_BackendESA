<?php
/**
 * Health Check Script
 */

header('Content-Type: application/json');

$results = [
    'timestamp' => date('c'),
    'checks' => [],
    'overall' => 'healthy'
];

// PHP Version
$results['checks']['php_version'] = [
    'status' => version_compare(phpversion(), '8.0.0', '>=') ? 'pass' : 'warn',
    'value' => phpversion()
];

// Required extensions
$extensions = ['pgsql', 'pdo_pgsql', 'json', 'curl', 'mbstring'];
foreach ($extensions as $ext) {
    $loaded = extension_loaded($ext);
    $results['checks']["ext_$ext"] = [
        'status' => $loaded ? 'pass' : 'fail',
        'value' => $loaded ? 'loaded' : 'missing'
    ];
    if (!$loaded) $results['overall'] = 'unhealthy';
}

// Environment file
$envFile = __DIR__ . '/.env';
$results['checks']['env_file'] = [
    'status' => file_exists($envFile) ? 'pass' : 'fail',
    'value' => file_exists($envFile) ? 'exists' : 'missing'
];

// Database connection
if (file_exists($envFile) && file_exists(__DIR__ . '/vendor/autoload.php')) {
    try {
        require_once __DIR__ . '/vendor/autoload.php';
        $dotenv = Dotenv\Dotenv::createImmutable(__DIR__);
        $dotenv->load();

        $dsn = sprintf(
            "pgsql:host=%s;port=%s;dbname=%s;sslmode=%s",
            $_ENV['DB_HOST'] ?? '',
            $_ENV['DB_PORT'] ?? '5432',
            $_ENV['DB_NAME'] ?? '',
            $_ENV['DB_SSLMODE'] ?? 'require'
        );

        $pdo = new PDO($dsn, $_ENV['DB_USERNAME'] ?? '', $_ENV['DB_PASSWORD'] ?? '', [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_TIMEOUT => 5
        ]);

        $pdo->query("SELECT 1");
        $results['checks']['db_connection'] = ['status' => 'pass', 'value' => 'connected'];

    } catch (Exception $e) {
        $results['checks']['db_connection'] = [
            'status' => 'fail',
            'value' => 'failed',
            'error' => $e->getMessage()
        ];
        $results['overall'] = 'unhealthy';
    }
}

// Scheduler API
$schedulerUrl = $_ENV['SCHEDULER_API_URL'] ?? 'http://localhost:5000';
$ch = curl_init($schedulerUrl . '/health');
curl_setopt_array($ch, [CURLOPT_RETURNTRANSFER => true, CURLOPT_TIMEOUT => 3]);
curl_exec($ch);
$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
curl_close($ch);

$results['checks']['scheduler_api'] = [
    'status' => ($httpCode === 200) ? 'pass' : 'warn',
    'value' => ($httpCode === 200) ? 'reachable' : 'unreachable'
];

if ($results['overall'] === 'unhealthy') http_response_code(503);
echo json_encode($results, JSON_PRETTY_PRINT);
