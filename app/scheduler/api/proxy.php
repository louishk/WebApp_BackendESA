<?php
/**
 * Scheduler API Proxy
 * Forwards authenticated requests to Python scheduler API
 * Handles both API requests and Server-Sent Events (SSE)
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

// Check if this is a streaming request (SSE)
$isStreamRequest = strpos($path, '/stream') !== false;

// Get request method
$method = $_SERVER['REQUEST_METHOD'];

// Get request body for POST/PUT/PATCH
$body = null;
if (in_array($method, ['POST', 'PUT', 'PATCH'])) {
    $body = file_get_contents('php://input');
}

// Generate JWT token for backend-to-backend communication
$jwtToken = generateSchedulerToken($_SESSION['user']);

// Build headers
$headers = [
    'Authorization: Bearer ' . $jwtToken,
    'X-Forwarded-For: ' . ($_SERVER['REMOTE_ADDR'] ?? ''),
    'X-Forwarded-User: ' . ($_SESSION['user']['email'] ?? ''),
    'Content-Type: application/json',
];

if ($isStreamRequest) {
    // Handle Server-Sent Events streaming
    header('Content-Type: text/event-stream');
    header('Cache-Control: no-cache');
    header('X-Accel-Buffering: no');

    // Disable output buffering
    if (function_exists('apache_setenv')) {
        apache_setenv('no-gzip', '1');
    }
    ini_set('zlib.output_compression', 'Off');
    while (ob_get_level() > 0) {
        ob_end_flush();
    }

    // Stream the response
    $ch = curl_init($targetUrl);
    curl_setopt_array($ch, [
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_WRITEFUNCTION => function($ch, $data) {
            echo $data;
            flush();
            return strlen($data);
        },
        CURLOPT_TIMEOUT => 600, // 10 minute timeout for long-running jobs
        CURLOPT_CONNECTTIMEOUT => 5,
    ]);

    curl_exec($ch);
    $error = curl_error($ch);
    curl_close($ch);

    if ($error) {
        echo "data: [ERROR] Connection to scheduler failed: $error\n\n";
        echo "event: done\ndata: error\n\n";
    }

    exit;
}

// Standard API request
$ch = curl_init($targetUrl);

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
$contentType = curl_getinfo($ch, CURLINFO_CONTENT_TYPE);
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

if ($contentType) {
    header('Content-Type: ' . $contentType);
} else {
    header('Content-Type: application/json');
}

echo $response;
