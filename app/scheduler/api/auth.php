<?php
/**
 * Scheduler API Authentication Helper
 * Provides JWT validation and role checking for scheduler API requests
 */
require_once __DIR__ . '/../../../config.php';

/**
 * Validate JWT token from Authorization header or query parameter
 * @return array|null User payload if valid, null otherwise
 */
function validateApiAuth(): ?array {
    // Check Authorization header first
    $authHeader = $_SERVER['HTTP_AUTHORIZATION'] ?? '';
    $token = '';

    if (preg_match('/Bearer\s+(.+)/i', $authHeader, $matches)) {
        $token = $matches[1];
    } elseif (isset($_GET['token'])) {
        // Fallback to query parameter
        $token = $_GET['token'];
    }

    if (empty($token)) {
        return null;
    }

    return validateSchedulerToken($token);
}

/**
 * Require API authentication with specific roles
 * Exits with 401/403 if unauthorized
 * @param array $allowedRoles Roles that can access this endpoint
 * @return array User payload
 */
function requireApiAuth(array $allowedRoles = ['admin', 'scheduler_admin']): array {
    $user = validateApiAuth();

    if ($user === null) {
        http_response_code(401);
        header('Content-Type: application/json');
        echo json_encode(['error' => 'Unauthorized', 'message' => 'Invalid or missing authentication token']);
        exit;
    }

    if (!in_array($user['role'], $allowedRoles, true)) {
        http_response_code(403);
        header('Content-Type: application/json');
        echo json_encode(['error' => 'Forbidden', 'message' => 'Insufficient permissions']);
        exit;
    }

    return $user;
}

/**
 * Generate a new JWT token for API response
 * @param array $user User data
 * @return string JWT token
 */
function generateApiToken(array $user): string {
    return generateSchedulerToken($user);
}
