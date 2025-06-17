<?php
require_once __DIR__ . '/vendor/autoload.php';
if (session_status() === PHP_SESSION_NONE) {
    session_start();
}

// ─────────────────────────────────────────────────────────────
// Load .env
// ─────────────────────────────────────────────────────────────
$dotenv = Dotenv\Dotenv::createImmutable(__DIR__);
$dotenv->load();

// ─────────────────────────────────────────────────────────────
// Database Configuration
// ─────────────────────────────────────────────────────────────
$dbHost     = $_ENV['DB_HOST']     ?? 'localhost';
$dbUsername = $_ENV['DB_USERNAME'] ?? 'root';
$dbPassword = $_ENV['DB_PASSWORD'] ?? '';
$dbName     = $_ENV['DB_NAME']     ?? '';
$dbNameBK   = $_ENV['DB_NAME_BK']     ?? '';

$dsn = "mysql:host={$dbHost};dbname={$dbName};charset=utf8mb4";

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
// WABA Configuration
// ─────────────────────────────────────────────────────────────
$wabaIdListRaw   = $_ENV['WABA_ID_LIST'] ?? '';
$wabaIds         = array_filter(array_map('trim', explode(',', $wabaIdListRaw)));
$wabaAccessToken = $_ENV['WABA_ACCESS_TOKEN'] ?? '';

// ─────────────────────────────────────────────────────────────
// RBS API Configuration
// ─────────────────────────────────────────────────────────────
$rbsApiBaseUrl = rtrim($_ENV['RBS_API_BASE_URL'] ?? '', '/');
$rbsApiBearer  = $_ENV['RBS_API_BEARER_TOKEN'] ?? '';

define('RBS_API_BASE',   $rbsApiBaseUrl);
define('RBS_API_BEARER', $rbsApiBearer);

// ─────────────────────────────────────────────────────────────
// FreshSales (FSS) API Configuration
// ─────────────────────────────────────────────────────────────
$fssApiKey = $_ENV['FSS_API_KEY'] ?? '';
$fssApiBaseUrl = $_ENV['FSS_API_URL'] ?? 'https://api.redboxstorage.hk/fss';

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
// Role-Based Access Control (RBAC)
// ─────────────────────────────────────────────────────────────
function require_role($roles) {
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }
    if (!isset($_SESSION['user']) || !in_array($_SESSION['user']['role'], (array)$roles, true)) {
        header('HTTP/1.0 403 Forbidden');
        exit('Access denied');
    }
}
