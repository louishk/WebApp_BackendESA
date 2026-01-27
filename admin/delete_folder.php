<?php
session_start();
require __DIR__ . '/../config.php';
require_role('admin'); // Only admins can delete folders

$rel = $_GET['path'] ?? '';
if (!preg_match('#^[a-z0-9\-/]+$#', $rel)) {
    exit('Invalid folder path');
}

$baseDir = realpath(__DIR__ . '/../pages');
$target  = $baseDir . DIRECTORY_SEPARATOR . $rel;

// 1) Delete any DB pages in the folder
$pdo->prepare("DELETE FROM pages WHERE slug LIKE ?")
    ->execute([$rel . '/%']);

// 2) Recursively delete files/folders
function deleteFolder(string $path) {
    foreach (glob($path . '/*') as $item) {
        is_dir($item) ? deleteFolder($item) : @unlink($item);
    }
    @rmdir($path);
}
deleteFolder($target);

// 3) Remove empty parent folders up to base
$parent = dirname($target);
while ($parent !== $baseDir) {
    if (is_dir($parent) && count(glob($parent . '/*')) === 0) {
        @rmdir($parent);
        $parent = dirname($parent);
    } else {
        break;
    }
}

header('Location: list_pages.php');
exit;