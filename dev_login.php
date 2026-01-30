<?php
/**
 * Development Login - Creates a dummy admin session for local testing
 * DO NOT deploy this file to production!
 */

// Only allow on localhost
$allowedHosts = ['localhost', '127.0.0.1', '::1'];
if (!in_array($_SERVER['HTTP_HOST'] ?? '', $allowedHosts) &&
    !in_array($_SERVER['SERVER_NAME'] ?? '', $allowedHosts)) {
    http_response_code(403);
    die('Dev login only available on localhost');
}

session_start();

// Create dummy admin user session
$_SESSION['user'] = [
    'id' => 1,
    'username' => 'dev_admin',
    'name' => 'Dev Admin',
    'email' => 'dev@localhost',
    'role' => 'admin',
];

// Redirect to scheduler or dashboard
$redirect = $_GET['to'] ?? '/app/scheduler/';
header('Location: ' . $redirect);
exit;
