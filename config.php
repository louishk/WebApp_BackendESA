<?php
/**
 * WebApp Backend Configuration
 * PostgreSQL Database + JWT Authentication
 */

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/app/Vault/SecretsVault.php';

use Firebase\JWT\JWT;
use Firebase\JWT\Key;
use function App\Vault\getSecret;

if (session_status() === PHP_SESSION_NONE) {
    session_start();
}

// ─────────────────────────────────────────────────────────────
// Load .env
// ─────────────────────────────────────────────────────────────
$dotenv = Dotenv\Dotenv::createImmutable(__DIR__);
$dotenv->load();

// ─────────────────────────────────────────────────────────────
// PostgreSQL Database Configuration
// Sensitive values loaded from vault, fallback to .env
// ─────────────────────────────────────────────────────────────
$dbHost     = $_ENV['DB_HOST'] ?? 'localhost';
$dbPort     = $_ENV['DB_PORT'] ?? '5432';
$dbName     = $_ENV['DB_NAME'] ?? 'backend';
$dbUsername = $_ENV['DB_USERNAME'] ?? '';
$dbPassword = getSecret('DB_PASSWORD', $_ENV['DB_PASSWORD'] ?? '');
$dbSslMode  = $_ENV['DB_SSLMODE'] ?? 'require';

// PostgreSQL DSN with SSL for Azure
$dsn = "pgsql:host={$dbHost};port={$dbPort};dbname={$dbName};sslmode={$dbSslMode}";

try {
    $pdo = new PDO($dsn, $dbUsername, $dbPassword, [
        PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES   => false,
    ]);
} catch (PDOException $e) {
    error_log("Database connection failed: " . $e->getMessage());
    die("Database connection error.");
}

// ─────────────────────────────────────────────────────────────
// JWT Configuration (secret loaded from vault)
// ─────────────────────────────────────────────────────────────
$jwtSecret = getSecret('JWT_SECRET', $_ENV['JWT_SECRET'] ?? '');
$jwtExpiry = (int)($_ENV['JWT_EXPIRY'] ?? 3600);

// ─────────────────────────────────────────────────────────────
// Scheduler Configuration
// ─────────────────────────────────────────────────────────────
$schedulerApiUrl = $_ENV['SCHEDULER_API_URL'] ?? 'http://localhost:5000';
$GLOBALS['schedulerApiUrl'] = $schedulerApiUrl;

// ─────────────────────────────────────────────────────────────
// Microsoft OAuth Configuration (secret loaded from vault)
// ─────────────────────────────────────────────────────────────
$azureConfig = [
    'clientId'     => $_ENV['MS_OAUTH_CLIENT_ID']     ?? '',
    'clientSecret' => getSecret('MS_OAUTH_CLIENT_SECRET', $_ENV['MS_OAUTH_CLIENT_SECRET'] ?? ''),
    'redirectUri'  => $_ENV['MS_OAUTH_REDIRECT_URI']  ?? '',
    'tenant'       => $_ENV['MS_OAUTH_TENANT']        ?? 'common',
];

// ─────────────────────────────────────────────────────────────
// JWT Helper Functions
// ─────────────────────────────────────────────────────────────

/**
 * Generate a JWT token for scheduler API access
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
 * Validate a JWT token and return the payload
 */
function validateSchedulerToken(string $token): ?array {
    global $jwtSecret;

    try {
        $decoded = JWT::decode($token, new Key($jwtSecret, 'HS256'));
        return (array)$decoded;
    } catch (Exception $e) {
        error_log("JWT validation failed: " . $e->getMessage());
        return null;
    }
}

/**
 * Get JWT token from Authorization header
 */
function getTokenFromHeader(): ?string {
    $headers = getallheaders();
    $authHeader = $headers['Authorization'] ?? '';

    if (preg_match('/Bearer\s+(.+)$/i', $authHeader, $matches)) {
        return $matches[1];
    }

    return null;
}

// ─────────────────────────────────────────────────────────────
// Role-Based Access Control (RBAC)
// ─────────────────────────────────────────────────────────────

/**
 * Require user to have one of the specified roles
 * Roles: admin, scheduler_admin, editor, viewer
 */
function require_role($roles): void {
    $allowedRoles = (array)$roles;

    if (!isset($_SESSION['user'])) {
        header('Location: /login.php');
        exit();
    }

    if (!in_array($_SESSION['user']['role'], $allowedRoles, true)) {
        header('HTTP/1.0 403 Forbidden');
        exit('Access denied');
    }
}

/**
 * Check if current user has a specific role
 */
function has_role($roles): bool {
    $allowedRoles = (array)$roles;
    return isset($_SESSION['user']) && in_array($_SESSION['user']['role'], $allowedRoles, true);
}

/**
 * Check if user is logged in
 */
function is_logged_in(): bool {
    return isset($_SESSION['user']) && !empty($_SESSION['user']['id']);
}

// ─────────────────────────────────────────────────────────────
// Utility Functions
// ─────────────────────────────────────────────────────────────

/**
 * Sanitize output for HTML
 */
function h($str): string {
    return htmlspecialchars($str ?? '', ENT_QUOTES, 'UTF-8');
}
