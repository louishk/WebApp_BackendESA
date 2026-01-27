<?php
/**
 * Scheduler API Proxy
 * Forwards authenticated requests from PHP to Python scheduler API
 */
require_once __DIR__ . '/auth.php';

// For session-based auth (from web UI)
if (session_status() === PHP_SESSION_NONE) {
    session_start();
}

// Validate authentication (session or JWT)
$user = null;
if (isset($_SESSION['user']) && hasSchedulerAccess()) {
    $user = $_SESSION['user'];
    // Generate fresh token for the request
    $jwtToken = $_SESSION['scheduler_token'] ?? generateSchedulerToken($user);
} else {
    // Try JWT-based auth
    $user = validateApiAuth();
    if ($user === null) {
        http_response_code(401);
        header('Content-Type: application/json');
        echo json_encode(['error' => 'Unauthorized']);
        exit;
    }

    if (!in_array($user['role'], ['admin', 'scheduler_admin'], true)) {
        http_response_code(403);
        header('Content-Type: application/json');
        echo json_encode(['error' => 'Forbidden']);
        exit;
    }
    $jwtToken = $_SERVER['HTTP_AUTHORIZATION'] ?? '';
    $jwtToken = preg_replace('/^Bearer\s+/i', '', $jwtToken);
}

// Get scheduler API URL and path
$schedulerUrl = $_ENV['SCHEDULER_API_URL'] ?? 'http://localhost:5000';
$path = $_GET['path'] ?? '';
$path = ltrim($path, '/');

// Build target URL
$targetUrl = rtrim($schedulerUrl, '/') . '/api/' . $path;

// Get request body for POST/PUT/PATCH
$requestBody = file_get_contents('php://input');

// Initialize cURL
$ch = curl_init($targetUrl);

// Set cURL options
$curlOptions = [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_FOLLOWLOCATION => true,
    CURLOPT_TIMEOUT => 30,
    CURLOPT_HTTPHEADER => [
        'Authorization: Bearer ' . $jwtToken,
        'Content-Type: application/json',
        'Accept: application/json',
        'X-Forwarded-For: ' . ($_SERVER['REMOTE_ADDR'] ?? ''),
        'X-Original-User: ' . ($user['email'] ?? $user['sub'] ?? 'unknown'),
    ],
];

// Set method-specific options
switch ($_SERVER['REQUEST_METHOD']) {
    case 'POST':
        $curlOptions[CURLOPT_POST] = true;
        $curlOptions[CURLOPT_POSTFIELDS] = $requestBody;
        break;
    case 'PUT':
        $curlOptions[CURLOPT_CUSTOMREQUEST] = 'PUT';
        $curlOptions[CURLOPT_POSTFIELDS] = $requestBody;
        break;
    case 'PATCH':
        $curlOptions[CURLOPT_CUSTOMREQUEST] = 'PATCH';
        $curlOptions[CURLOPT_POSTFIELDS] = $requestBody;
        break;
    case 'DELETE':
        $curlOptions[CURLOPT_CUSTOMREQUEST] = 'DELETE';
        break;
    case 'GET':
    default:
        // GET is default
        break;
}

curl_setopt_array($ch, $curlOptions);

// Execute request
$response = curl_exec($ch);
$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$error = curl_error($ch);
curl_close($ch);

// Handle errors
if ($error) {
    http_response_code(502);
    header('Content-Type: application/json');
    echo json_encode([
        'error' => 'Bad Gateway',
        'message' => 'Failed to connect to scheduler API',
        'details' => $error
    ]);
    exit;
}

// Forward response
http_response_code($httpCode);
header('Content-Type: application/json');
echo $response;
