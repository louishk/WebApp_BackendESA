<?php
require_once __DIR__ . '/vendor/autoload.php';

use Firebase\JWT\JWT;
use Firebase\JWT\Key;

if (session_status() === PHP_SESSION_NONE) {
    session_start();
}

// ─────────────────────────────────────────────────────────────
// Load .env
// ─────────────────────────────────────────────────────────────
$dotenv = Dotenv\Dotenv::createImmutable(__DIR__);
$dotenv->load();

// ─────────────────────────────────────────────────────────────
// PostgreSQL Database Configuration (Backend)
// ─────────────────────────────────────────────────────────────
$dbHost     = $_ENV['DB_HOST']     ?? 'localhost';
$dbPort     = $_ENV['DB_PORT']     ?? '5432';
$dbUsername = $_ENV['DB_USERNAME'] ?? '';
$dbPassword = $_ENV['DB_PASSWORD'] ?? '';
$dbName     = $_ENV['DB_NAME']     ?? 'backend';
$dbSslMode  = $_ENV['DB_SSLMODE']  ?? 'require';

$dsn = "pgsql:host={$dbHost};port={$dbPort};dbname={$dbName};sslmode={$dbSslMode}";

try {
    $pdo = new PDO($dsn, $dbUsername, $dbPassword, [
        PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]);
} catch (PDOException $e) {
    error_log("PDO connection error: " . $e->getMessage());
    die("Database connection failed.");
}

// ─────────────────────────────────────────────────────────────
// PostgreSQL Data Layer Configuration (esa_pbi)
// ─────────────────────────────────────────────────────────────
$dataDbHost     = $_ENV['DATA_DB_HOST']     ?? $dbHost;
$dataDbPort     = $_ENV['DATA_DB_PORT']     ?? '5432';
$dataDbUsername = $_ENV['DATA_DB_USERNAME'] ?? $dbUsername;
$dataDbPassword = $_ENV['DATA_DB_PASSWORD'] ?? $dbPassword;
$dataDbName     = $_ENV['DATA_DB_NAME']     ?? 'esa_pbi';
$dataDbSslMode  = $_ENV['DATA_DB_SSLMODE']  ?? 'require';

/**
 * Get Data Layer PDO connection (esa_pbi database)
 */
function getDataLayerPdo(): PDO {
    global $dataDbHost, $dataDbPort, $dataDbUsername, $dataDbPassword, $dataDbName, $dataDbSslMode;

    static $dataPdo = null;
    if ($dataPdo === null) {
        $dsn = "pgsql:host={$dataDbHost};port={$dataDbPort};dbname={$dataDbName};sslmode={$dataDbSslMode}";
        $dataPdo = new PDO($dsn, $dataDbUsername, $dataDbPassword, [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ]);
    }
    return $dataPdo;
}

// ─────────────────────────────────────────────────────────────
// Microsoft OAuth Configuration
// ─────────────────────────────────────────────────────────────
$azureConfig = [
    'clientId'     => $_ENV['MS_OAUTH_CLIENT_ID']     ?? '',
    'clientSecret' => $_ENV['MS_OAUTH_CLIENT_SECRET'] ?? '',
    'redirectUri'  => $_ENV['MS_OAUTH_REDIRECT_URI']  ?? '',
    'tenant'       => $_ENV['MS_OAUTH_TENANT']        ?? 'common',
];

// ─────────────────────────────────────────────────────────────
// JWT Configuration
// ─────────────────────────────────────────────────────────────
$jwtSecret = $_ENV['JWT_SECRET'] ?? '';
$jwtExpiry = (int)($_ENV['JWT_EXPIRY'] ?? 3600);

/**
 * Generate a JWT token for scheduler access
 */
function generateSchedulerToken(array $user): string {
    global $jwtSecret, $jwtExpiry;

    $payload = [
        'iss'   => 'webapp_backend',
        'sub'   => $user['id'],
        'email' => $user['email'] ?? '',
        'role'  => $user['role'],
        'iat'   => time(),
        'exp'   => time() + $jwtExpiry
    ];

    return JWT::encode($payload, $jwtSecret, 'HS256');
}

/**
 * Validate and decode a JWT token
 */
function validateSchedulerToken(string $token): ?array {
    global $jwtSecret;

    try {
        $decoded = JWT::decode($token, new Key($jwtSecret, 'HS256'));
        return (array)$decoded;
    } catch (Exception $e) {
        error_log("JWT validation error: " . $e->getMessage());
        return null;
    }
}

// ─────────────────────────────────────────────────────────────
// Scheduler Configuration
// ─────────────────────────────────────────────────────────────
$schedulerApiUrl = $_ENV['SCHEDULER_API_URL'] ?? 'http://localhost:5000';

// ─────────────────────────────────────────────────────────────
// Role-Based Access Control (RBAC)
// ─────────────────────────────────────────────────────────────
/**
 * Require user to have one of the specified roles
 * @param string|array $roles Single role or array of allowed roles
 */
function require_role($roles): void {
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    $allowedRoles = (array)$roles;

    if (!isset($_SESSION['user']) || !in_array($_SESSION['user']['role'], $allowedRoles, true)) {
        header('HTTP/1.0 403 Forbidden');
        exit('Access denied');
    }
}

/**
 * Check if current user has scheduler access (admin or scheduler_admin)
 */
function hasSchedulerAccess(): bool {
    return isset($_SESSION['user']) &&
           in_array($_SESSION['user']['role'], ['admin', 'scheduler_admin'], true);
}
