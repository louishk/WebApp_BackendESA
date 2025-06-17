<?php
// Enable error reporting for debugging (disable in production)
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

session_start();
require 'config.php';

// Only accept POST requests
if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    header('Location: index.php');
    exit;
}

// Fetch and sanitize input
$username = trim($_POST['username'] ?? '');
$password = $_POST['password'] ?? '';

// Validate presence
if (!$username || !$password) {
    header('Location: index.php?error=empty');
    exit;
}

try {
    // Fetch user record (note: users.password, not password_hash)
    $stmt = $pdo->prepare("SELECT id, username, password, role FROM users WHERE username = ?");
    $stmt->execute([$username]);
    $user = $stmt->fetch(PDO::FETCH_ASSOC);

    if ($user && password_verify($password, $user['password'])) {
        $_SESSION['user'] = [
            'id'       => $user['id'],
            'username' => $user['username'],
            'role'     => $user['role']
        ];
        header("Location: dashboard.php");
        exit();
    } else {
        $error = "Invalid username or password.";
    }
    // Invalid credentials
    header('Location: index.php?error=invalid');
    exit;
} catch (PDOException $e) {
    // Log the error and show generic message
    error_log('Login error: ' . $e->getMessage());
    http_response_code(500);
    echo 'Server error. Please check the logs.';
    exit;
}
