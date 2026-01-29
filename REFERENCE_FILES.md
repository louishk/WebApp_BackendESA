# Reference Files - Full Content

This document contains the complete content of key files that were created/modified.

---

## 1. config.php (Complete)

```php
<?php
/**
 * WebApp Backend Configuration
 * PostgreSQL Database + JWT Authentication
 */

require_once __DIR__ . '/vendor/autoload.php';

use Firebase\JWT\JWT;
use Firebase\JWT\Key;

// ─────────────────────────────────────────────────────────────
// Environment Loading
// ─────────────────────────────────────────────────────────────

$dotenv = Dotenv\Dotenv::createImmutable(__DIR__);
$dotenv->load();

// ─────────────────────────────────────────────────────────────
// Database Configuration (PostgreSQL)
// ─────────────────────────────────────────────────────────────

$dbHost     = $_ENV['DB_HOST'] ?? 'localhost';
$dbPort     = $_ENV['DB_PORT'] ?? '5432';
$dbName     = $_ENV['DB_NAME'] ?? 'backend';
$dbUsername = $_ENV['DB_USERNAME'] ?? '';
$dbPassword = $_ENV['DB_PASSWORD'] ?? '';
$dbSslMode  = $_ENV['DB_SSLMODE'] ?? 'require';

// ─────────────────────────────────────────────────────────────
// JWT Configuration
// ─────────────────────────────────────────────────────────────

$jwtSecret = $_ENV['JWT_SECRET'] ?? '';
$jwtExpiry = (int)($_ENV['JWT_EXPIRY'] ?? 3600);

// ─────────────────────────────────────────────────────────────
// Scheduler Configuration
// ─────────────────────────────────────────────────────────────

$schedulerApiUrl = $_ENV['SCHEDULER_API_URL'] ?? 'http://localhost:5000';

// ─────────────────────────────────────────────────────────────
// Database Connection
// ─────────────────────────────────────────────────────────────

// PostgreSQL DSN with SSL mode for Azure
$dsn = "pgsql:host={$dbHost};port={$dbPort};dbname={$dbName};sslmode={$dbSslMode}";

try {
    $pdo = new PDO($dsn, $dbUsername, $dbPassword, [
        PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES   => false,
    ]);
} catch (PDOException $e) {
    error_log("Database connection failed: " . $e->getMessage());
    die("Database connection error. Please check configuration.");
}

// ─────────────────────────────────────────────────────────────
// Session Configuration
// ─────────────────────────────────────────────────────────────

if (session_status() === PHP_SESSION_NONE) {
    session_start();
}

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
        header('HTTP/1.0 401 Unauthorized');
        header('Location: /login.php');
        exit();
    }

    if (!in_array($_SESSION['user']['role'], $allowedRoles, true)) {
        header('HTTP/1.0 403 Forbidden');
        exit('Access denied. Required role: ' . implode(' or ', $allowedRoles));
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

/**
 * Get current user or null
 */
function current_user(): ?array {
    return $_SESSION['user'] ?? null;
}

// ─────────────────────────────────────────────────────────────
// Audit Logging
// ─────────────────────────────────────────────────────────────

/**
 * Log an action to the audit log
 */
function audit_log(string $action, ?string $resource = null, ?array $details = null): void {
    global $pdo;

    $userId = $_SESSION['user']['id'] ?? null;
    $ipAddress = $_SERVER['REMOTE_ADDR'] ?? null;

    try {
        $stmt = $pdo->prepare("
            INSERT INTO audit_log (user_id, action, resource, details, ip_address)
            VALUES (?, ?, ?, ?, ?)
        ");
        $stmt->execute([
            $userId,
            $action,
            $resource,
            $details ? json_encode($details) : null,
            $ipAddress
        ]);
    } catch (PDOException $e) {
        error_log("Audit log failed: " . $e->getMessage());
    }
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

/**
 * Generate CSRF token
 */
function csrf_token(): string {
    if (empty($_SESSION['csrf_token'])) {
        $_SESSION['csrf_token'] = bin2hex(random_bytes(32));
    }
    return $_SESSION['csrf_token'];
}

/**
 * Verify CSRF token
 */
function verify_csrf(string $token): bool {
    return isset($_SESSION['csrf_token']) && hash_equals($_SESSION['csrf_token'], $token);
}
```

---

## 2. app/scheduler/index.php (Complete)

```php
<?php
/**
 * Scheduler App Entry Point
 * Requires authentication and embeds scheduler UI
 */

require_once __DIR__ . '/../../config.php';

// Require admin or scheduler_admin role
require_role(['admin', 'scheduler_admin']);

// Get or generate scheduler token
$schedulerToken = $_SESSION['scheduler_token'] ?? null;
if (!$schedulerToken) {
    $schedulerToken = generateSchedulerToken($_SESSION['user']);
    $_SESSION['scheduler_token'] = $schedulerToken;
}

// Scheduler API URL
$schedulerApiUrl = $GLOBALS['schedulerApiUrl'] ?? 'http://localhost:5000';

?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PBI Scheduler - Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
        .header {
            background: #1a1a2e;
            color: white;
            padding: 1rem 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .header h1 { font-size: 1.5rem; }
        .header-nav a {
            color: white;
            text-decoration: none;
            margin-left: 1.5rem;
            opacity: 0.8;
        }
        .header-nav a:hover { opacity: 1; }
        .scheduler-frame {
            width: 100%;
            height: calc(100vh - 60px);
            border: none;
        }
        .user-info {
            display: flex;
            align-items: center;
            gap: 1rem;
        }
        .user-badge {
            background: rgba(255,255,255,0.1);
            padding: 0.25rem 0.75rem;
            border-radius: 1rem;
            font-size: 0.875rem;
        }
    </style>
    <script>
        // Make token available to scheduler UI
        window.SCHEDULER_TOKEN = '<?= htmlspecialchars($schedulerToken) ?>';
        window.SCHEDULER_API = '/app/scheduler/api/proxy.php?path=';
    </script>
</head>
<body>
    <header class="header">
        <h1>PBI Scheduler</h1>
        <div class="user-info">
            <span class="user-badge"><?= h($_SESSION['user']['role']) ?></span>
            <span><?= h($_SESSION['user']['username'] ?? $_SESSION['user']['email']) ?></span>
        </div>
        <nav class="header-nav">
            <a href="/dashboard.php">Dashboard</a>
            <a href="/logout.php">Logout</a>
        </nav>
    </header>

    <iframe
        src="<?= htmlspecialchars($schedulerApiUrl) ?>"
        class="scheduler-frame"
        title="Scheduler Dashboard"
    ></iframe>
</body>
</html>
```

---

## 3. app/scheduler/api/proxy.php (Complete)

```php
<?php
/**
 * Scheduler API Proxy
 * Forwards authenticated requests to Python scheduler API
 */

require_once __DIR__ . '/auth.php';

// Require authentication
requireSchedulerAuth();

// Get path from query string
$path = $_GET['path'] ?? '';
$path = ltrim($path, '/');

// Build target URL
$schedulerUrl = $GLOBALS['schedulerApiUrl'] ?? 'http://localhost:5000';
$targetUrl = $schedulerUrl . '/api/' . $path;

// Get request method
$method = $_SERVER['REQUEST_METHOD'];

// Get request body for POST/PUT/PATCH
$body = null;
if (in_array($method, ['POST', 'PUT', 'PATCH'])) {
    $body = file_get_contents('php://input');
}

// Generate fresh JWT token for backend-to-backend communication
$jwtToken = generateSchedulerToken($_SESSION['user']);

// Forward request via cURL
$ch = curl_init($targetUrl);

$headers = [
    'Authorization: Bearer ' . $jwtToken,
    'Content-Type: application/json',
    'Accept: application/json',
    'X-Forwarded-For: ' . ($_SERVER['REMOTE_ADDR'] ?? ''),
    'X-Forwarded-User: ' . ($_SESSION['user']['email'] ?? ''),
];

curl_setopt_array($ch, [
    CURLOPT_CUSTOMREQUEST => $method,
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_HTTPHEADER => $headers,
    CURLOPT_TIMEOUT => 30,
    CURLOPT_CONNECTTIMEOUT => 5,
]);

if ($body !== null) {
    curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
}

// Execute request
$response = curl_exec($ch);
$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$error = curl_error($ch);
curl_close($ch);

// Handle errors
if ($error) {
    http_response_code(502);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'Scheduler API unavailable', 'details' => $error]);
    exit;
}

// Forward response
http_response_code($httpCode);
header('Content-Type: application/json');
echo $response;
```

---

## 4. app/scheduler/api/auth.php (Complete)

```php
<?php
/**
 * Scheduler Authentication Helpers
 */

require_once __DIR__ . '/../../../config.php';

/**
 * Require scheduler authentication
 * Checks session and optionally JWT token
 */
function requireSchedulerAuth(): void {
    // First check session
    if (isset($_SESSION['user']) && has_role(['admin', 'scheduler_admin'])) {
        return; // Authenticated via session
    }

    // Then check JWT token in header
    $token = getTokenFromHeader();
    if ($token) {
        $payload = validateSchedulerToken($token);
        if ($payload && in_array($payload['role'] ?? '', ['admin', 'scheduler_admin'])) {
            // Set session from token
            $_SESSION['user'] = [
                'id'    => $payload['sub'],
                'email' => $payload['email'] ?? '',
                'role'  => $payload['role'],
            ];
            return; // Authenticated via JWT
        }
    }

    // Not authenticated
    http_response_code(401);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'Unauthorized', 'message' => 'Valid authentication required']);
    exit;
}

/**
 * Get current authenticated user from session or token
 */
function getSchedulerUser(): ?array {
    if (isset($_SESSION['user'])) {
        return $_SESSION['user'];
    }

    $token = getTokenFromHeader();
    if ($token) {
        return validateSchedulerToken($token);
    }

    return null;
}
```

---

## 5. app/scheduler/python/web/auth.py (Complete)

```python
"""
JWT Authentication Middleware for Flask Scheduler API
"""

import os
import jwt
import logging
from functools import wraps
from flask import request, jsonify, g
from datetime import datetime

# Configuration
JWT_SECRET = os.environ.get('JWT_SECRET', '')
JWT_ALGORITHM = os.environ.get('JWT_ALGORITHM', 'HS256')

# Roles that can access scheduler
SCHEDULER_ROLES = ['admin', 'scheduler_admin']

logger = logging.getLogger(__name__)


class AuthError(Exception):
    """Authentication error with status code"""
    def __init__(self, message: str, status_code: int = 401):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


def init_auth(app):
    """Initialize authentication for Flask app"""
    global JWT_SECRET, JWT_ALGORITHM

    JWT_SECRET = app.config.get('JWT_SECRET', os.environ.get('JWT_SECRET', ''))
    JWT_ALGORITHM = app.config.get('JWT_ALGORITHM', os.environ.get('JWT_ALGORITHM', 'HS256'))

    if not JWT_SECRET:
        logger.warning("JWT_SECRET not configured - authentication will fail")


def get_token_from_header() -> str | None:
    """Extract JWT token from Authorization header"""
    auth_header = request.headers.get('Authorization', '')

    if auth_header.startswith('Bearer '):
        return auth_header[7:]

    return None


def decode_token(token: str) -> dict:
    """Decode and validate JWT token"""
    if not JWT_SECRET:
        raise AuthError('JWT not configured', 500)

    try:
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=[JWT_ALGORITHM],
            options={'require': ['exp', 'sub', 'role']}
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise AuthError('Token expired', 401)
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {e}")
        raise AuthError('Invalid token', 401)


def require_auth(f):
    """Decorator to require authentication for a route"""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = get_token_from_header()

        if not token:
            return jsonify({'error': 'Unauthorized', 'message': 'No token provided'}), 401

        try:
            payload = decode_token(token)

            # Check role
            role = payload.get('role')
            if role not in SCHEDULER_ROLES:
                return jsonify({
                    'error': 'Forbidden',
                    'message': f'Role {role} not authorized for scheduler access'
                }), 403

            # Store user in Flask g object
            g.current_user = payload

        except AuthError as e:
            return jsonify({'error': 'Unauthorized', 'message': e.message}), e.status_code

        return f(*args, **kwargs)

    return decorated


def optional_auth(f):
    """Decorator to optionally authenticate (for public endpoints with optional auth)"""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = get_token_from_header()

        if token:
            try:
                payload = decode_token(token)
                g.current_user = payload
            except AuthError:
                g.current_user = None
        else:
            g.current_user = None

        return f(*args, **kwargs)

    return decorated


def get_current_user() -> dict | None:
    """Get current authenticated user from Flask g object"""
    return getattr(g, 'current_user', None)


def is_authenticated() -> bool:
    """Check if current request is authenticated"""
    return get_current_user() is not None
```

---

## 6. app/scheduler/python/.env (Complete)

```env
# PBI Scheduler Environment Configuration
# This scheduler uses two databases:
# 1. Backend DB (backend) - for scheduler state, tokens, audit logs
# 2. Data Layer DB (esa_pbi) - for pipeline data tables

# PostgreSQL Backend Database (scheduler state tables)
# These variables are used by both the scheduler and Flask app
DB_HOST=esapbi.postgres.database.azure.com
DB_PORT=5432
DB_NAME=backend
DB_USER=esa_pbi_admin
DB_USERNAME=esa_pbi_admin
DB_PASSWORD=K9wKmtRfj3zJqRU
DB_SSLMODE=require

# PostgreSQL Data Layer Database (pipeline data)
DATA_DB_HOST=esapbi.postgres.database.azure.com
DATA_DB_PORT=5432
DATA_DB_NAME=esa_pbi
DATA_DB_USER=esa_pbi_admin
DATA_DB_PASSWORD=K9wKmtRfj3zJqRU
DATA_DB_SSLMODE=require

# Legacy variable names (for compatibility)
POSTGRESQL_HOST=esapbi.postgres.database.azure.com
POSTGRESQL_PORT=5432
POSTGRESQL_DATABASE=backend
POSTGRESQL_USERNAME=esa_pbi_admin
POSTGRESQL_PASSWORD=K9wKmtRfj3zJqRU

# JWT Authentication (must match PHP backend)
JWT_SECRET=x7K9mP2vQ8wR4tY6uB3nC5hJ1fE0gA9s
JWT_ALGORITHM=HS256

# Web UI
FLASK_HOST=127.0.0.1
FLASK_PORT=5000
FLASK_DEBUG=false

# Logging
LOG_LEVEL=INFO
LOG_FILE=/var/log/pbi-scheduler/scheduler.log

# Config paths (use relative paths in production)
# CONFIG_DIR=./config
```

---

## 7. sql/setup_postgresql.sql (Complete)

```sql
-- ============================================
-- WebApp Backend PostgreSQL Schema
-- ============================================
-- Run this script to set up the backend database
--
-- Usage:
-- psql "host=esapbi.postgres.database.azure.com port=5432 dbname=backend user=esa_pbi_admin sslmode=require" -f setup_postgresql.sql
-- ============================================

-- ─────────────────────────────────────────────────────────────
-- Users Table
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    email VARCHAR(255) UNIQUE,
    password VARCHAR(255),
    role VARCHAR(20) NOT NULL CHECK (role IN ('admin', 'scheduler_admin', 'editor', 'viewer')),
    auth_provider VARCHAR(20) DEFAULT 'local' CHECK (auth_provider IN ('local', 'microsoft')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ─────────────────────────────────────────────────────────────
-- Pages Table
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pages (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    slug VARCHAR(255) UNIQUE NOT NULL,
    content TEXT,
    is_secure BOOLEAN DEFAULT FALSE,
    extension VARCHAR(10) DEFAULT 'php',
    edit_restricted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ─────────────────────────────────────────────────────────────
-- Schema Markups Table (for SEO tool)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_markups (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    schema_type VARCHAR(100) NOT NULL,
    schema_data JSONB NOT NULL,
    form_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ─────────────────────────────────────────────────────────────
-- Scheduler Tokens Table
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS scheduler_tokens (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token VARCHAR(512) NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_scheduler_tokens_user ON scheduler_tokens(user_id);
CREATE INDEX IF NOT EXISTS idx_scheduler_tokens_expires ON scheduler_tokens(expires_at);

-- ─────────────────────────────────────────────────────────────
-- Audit Log Table
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id) ON DELETE SET NULL,
    action VARCHAR(100) NOT NULL,
    resource VARCHAR(255),
    details JSONB,
    ip_address VARCHAR(45),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at);

-- ─────────────────────────────────────────────────────────────
-- Scheduler Tables (copied from esa_pbi)
-- ─────────────────────────────────────────────────────────────

-- Scheduler State
CREATE TABLE IF NOT EXISTS scheduler_state (
    id SERIAL PRIMARY KEY,
    key VARCHAR(255) UNIQUE NOT NULL,
    value JSONB,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Scheduler Pipeline Config
CREATE TABLE IF NOT EXISTS scheduler_pipeline_config (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255) UNIQUE NOT NULL,
    config JSONB NOT NULL,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Scheduler Job History
CREATE TABLE IF NOT EXISTS scheduler_job_history (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) NOT NULL,
    pipeline_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds FLOAT,
    error_message TEXT,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_job_history_pipeline ON scheduler_job_history(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_job_history_status ON scheduler_job_history(status);
CREATE INDEX IF NOT EXISTS idx_job_history_started ON scheduler_job_history(started_at);

-- Scheduler Resource Locks
CREATE TABLE IF NOT EXISTS scheduler_resource_locks (
    id SERIAL PRIMARY KEY,
    resource_name VARCHAR(255) UNIQUE NOT NULL,
    locked_by VARCHAR(255),
    locked_at TIMESTAMP,
    expires_at TIMESTAMP
);

-- APScheduler Jobs (for persistent job storage)
CREATE TABLE IF NOT EXISTS apscheduler_jobs (
    id VARCHAR(191) PRIMARY KEY,
    next_run_time FLOAT,
    job_state BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_apscheduler_next_run ON apscheduler_jobs(next_run_time);

-- ─────────────────────────────────────────────────────────────
-- Default Admin User
-- ─────────────────────────────────────────────────────────────
-- Password: admin123 (CHANGE THIS IN PRODUCTION!)
-- Hash generated with: password_hash('admin123', PASSWORD_BCRYPT)

INSERT INTO users (username, email, password, role, auth_provider)
VALUES (
    'admin',
    'admin@example.com',
    '$2y$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi',
    'admin',
    'local'
)
ON CONFLICT (username) DO NOTHING;

-- ─────────────────────────────────────────────────────────────
-- Triggers for updated_at
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_pages_updated_at ON pages;
CREATE TRIGGER update_pages_updated_at
    BEFORE UPDATE ON pages
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_schema_markups_updated_at ON schema_markups;
CREATE TRIGGER update_schema_markups_updated_at
    BEFORE UPDATE ON schema_markups
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ─────────────────────────────────────────────────────────────
-- Grant Permissions (if needed)
-- ─────────────────────────────────────────────────────────────

-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO esa_pbi_admin;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO esa_pbi_admin;

-- ============================================
-- Setup Complete
-- ============================================
```

---

## 8. healthcheck.php (Complete)

```php
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

        // Check 6: JWT Configuration
        $jwtSecret = $_ENV['JWT_SECRET'] ?? null;
        $results['checks']['jwt_config'] = [
            'status' => ($jwtSecret && strlen($jwtSecret) >= 32) ? 'pass' : 'warn',
            'value' => $jwtSecret ? (strlen($jwtSecret) >= 32 ? 'configured' : 'too short') : 'missing'
        ];

        // Check 7: Scheduler API
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
```

---

## 9. nginx Config for Backend + Scheduler

Save to `/etc/nginx/sites-available/backend`:

```nginx
server {
    server_name backend.extraspace.com.sg 57.158.27.35;

    root /var/www/backend;
    index index.php index.html;

    # Logging
    access_log /var/log/nginx/backend-access.log;
    error_log /var/log/nginx/backend-error.log;

    # PHP files
    location ~ \.php$ {
        include snippets/fastcgi-php.conf;
        fastcgi_pass unix:/var/run/php/php8.3-fpm.sock;
        fastcgi_param SCRIPT_FILENAME $document_root$fastcgi_script_name;
        include fastcgi_params;
    }

    # Default location
    location / {
        try_files $uri $uri/ /index.php?$query_string;
    }

    # Scheduler Web UI (direct proxy to Flask)
    location /scheduler/ {
        proxy_pass http://127.0.0.1:5000/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
        proxy_read_timeout 90;
    }

    # Static files caching
    location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|eot)$ {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    # Security - deny access to sensitive files
    location ~ /\.env { deny all; }
    location ~ /\.git { deny all; }
    location ~ /\.htaccess { deny all; }
    location ~ composer\.(json|lock)$ { deny all; }

    # SSL Configuration
    listen 443 ssl;
    ssl_certificate /etc/letsencrypt/live/backend.extraspace.com.sg/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/backend.extraspace.com.sg/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;
}

server {
    listen 80;
    server_name backend.extraspace.com.sg 57.158.27.35;

    # ACME challenges for Let's Encrypt
    location /.well-known/acme-challenge/ {
        root /var/www/html;
    }

    # Redirect HTTP to HTTPS
    location / {
        return 301 https://$host$request_uri;
    }
}
```
