<?php
session_start();
require __DIR__ . '/../config.php';
require_role('admin'); // Only admins may delete users

$id = isset($_GET['id']) ? (int)$_GET['id'] : 0;

// Prevent deleting own account
if ($id > 0 && $id !== (int)$_SESSION['user']['id']) {
    $pdo->prepare("DELETE FROM users WHERE id = ?")->execute([$id]);
}

header('Location: list_users.php');
exit;