<?php
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
    // Fetch user record - supports login by username or email
    $stmt = $pdo->prepare("SELECT id, username, email, password, role FROM users WHERE username = ? OR email = ?");
    $stmt->execute([$username, $username]);
    $user = $stmt->fetch(PDO::FETCH_ASSOC);

    if ($user && password_verify($password, $user['password'])) {
        $_SESSION['user'] = [
            'id'       => $user['id'],
            'username' => $user['username'],
            'email'    => $user['email'],
            'role'     => $user['role'],
            'auth'     => 'local',
        ];

        // Generate JWT token for scheduler access (admin/scheduler_admin only)
        if (in_array($user['role'], ['admin', 'scheduler_admin'])) {
            $_SESSION['scheduler_token'] = generateSchedulerToken($_SESSION['user']);
        }

        header("Location: dashboard.php");
        exit;
    }

    // Invalid credentials
    header('Location: index.php?error=invalid');
    exit;

} catch (PDOException $e) {
    error_log('Login error: ' . $e->getMessage());
    header('Location: index.php?error=server');
    exit;
}
