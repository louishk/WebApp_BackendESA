<?php
/**
 * Static Asset Proxy for Scheduler
 * Proxies static files from Flask backend
 */

require_once __DIR__ . '/../../config.php';

// Require admin or scheduler_admin role
require_role(['admin', 'scheduler_admin']);

// Get the requested file path
$file = $_GET['file'] ?? '';
$file = basename($file); // Security: prevent directory traversal

if (empty($file)) {
    http_response_code(400);
    exit('Missing file parameter');
}

// Scheduler URL
$schedulerUrl = $GLOBALS['schedulerApiUrl'] ?? 'http://localhost:5000';
$targetUrl = $schedulerUrl . '/static/' . $file;

// Fetch the static file
$ch = curl_init($targetUrl);
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_TIMEOUT => 10,
    CURLOPT_CONNECTTIMEOUT => 5,
]);

$content = curl_exec($ch);
$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$contentType = curl_getinfo($ch, CURLINFO_CONTENT_TYPE);
$error = curl_error($ch);
curl_close($ch);

if ($error || $httpCode >= 400) {
    http_response_code($httpCode ?: 404);
    exit('File not found');
}

// Set appropriate content type
if ($contentType) {
    header('Content-Type: ' . $contentType);
} else {
    // Guess from extension
    $ext = strtolower(pathinfo($file, PATHINFO_EXTENSION));
    $mimeTypes = [
        'jpeg' => 'image/jpeg',
        'jpg' => 'image/jpeg',
        'png' => 'image/png',
        'gif' => 'image/gif',
        'svg' => 'image/svg+xml',
        'css' => 'text/css',
        'js' => 'application/javascript',
    ];
    header('Content-Type: ' . ($mimeTypes[$ext] ?? 'application/octet-stream'));
}

// Cache static assets
header('Cache-Control: public, max-age=86400');

echo $content;
