<?php
session_start();
require __DIR__ . '/../config.php';
require_role('admin'); // Only admins can delete pages

$id = isset($_GET['id']) ? (int)$_GET['id'] : 0;
if ($id < 1) {
    header('Location: list_pages.php');
    exit;
}

$stmt = $pdo->prepare("SELECT slug, extension FROM pages WHERE id = ?");
$stmt->execute([$id]);
$page = $stmt->fetch(PDO::FETCH_ASSOC);

if ($page) {
    $baseDir = realpath(__DIR__ . '/../pages');
    $file    = $baseDir . DIRECTORY_SEPARATOR . $page['slug'] . '.' . $page['extension'];

    if (file_exists($file) && is_file($file)) {
        @unlink($file);
    }

    $parent = dirname($file);
    while ($parent && strpos($parent, $baseDir) === 0) {
        if (realpath($parent) === $baseDir) {
            break;
        }
        if (is_dir($parent) && count(glob($parent . DIRECTORY_SEPARATOR . '*')) === 0) {
            @rmdir($parent);
            $parent = dirname($parent);
        } else {
            break;
        }
    }

    $pdo->prepare("DELETE FROM pages WHERE id = ?")->execute([$id]);
}

header('Location: list_pages.php');
exit;
