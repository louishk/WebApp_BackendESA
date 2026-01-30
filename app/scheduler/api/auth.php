<?php
/**
 * Scheduler Authentication Helpers
 */

require_once __DIR__ . '/../../../config.php';

/**
 * Require scheduler authentication
 */
function requireSchedulerAuth(): void {
    // Check session first
    if (isset($_SESSION['user']) && has_role(['admin', 'scheduler_admin'])) {
        return;
    }

    // Check JWT token in header
    $token = getTokenFromHeader();
    if ($token) {
        $payload = validateSchedulerToken($token);
        if ($payload && in_array($payload['role'] ?? '', ['admin', 'scheduler_admin'])) {
            $_SESSION['user'] = [
                'id'    => $payload['sub'],
                'email' => $payload['email'] ?? '',
                'role'  => $payload['role'],
            ];
            return;
        }
    }

    // Not authenticated
    http_response_code(401);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'Unauthorized']);
    exit;
}

/**
 * Get current authenticated user
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
