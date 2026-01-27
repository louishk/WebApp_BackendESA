<?php
session_start();
require 'config.php';

if (!isset($_SESSION['user'])) {
    header('Location: index.php');
    exit;
}

$user = $_SESSION['user'];
$isAdmin = $user['role'] === 'admin';
$hasSchedulerAccess = in_array($user['role'], ['admin', 'scheduler_admin']);
?>
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>User Dashboard</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <nav class="navbar navbar-expand-lg navbar-light bg-white shadow-sm">
    <div class="container">
      <a class="navbar-brand" href="dashboard.php">Backend Portal</a>
      <div class="collapse navbar-collapse">
        <ul class="navbar-nav ms-auto">
          <?php if ($isAdmin || $hasSchedulerAccess): ?>
            <li class="nav-item">
              <a class="nav-link" href="admin/admin.php">Admin</a>
            </li>
          <?php endif; ?>
          <li class="nav-item">
            <a class="nav-link" href="logout.php">Logout</a>
          </li>
        </ul>
      </div>
    </div>
  </nav>

  <div class="container py-5">
    <h1>Welcome, <?= htmlspecialchars($user['username'], ENT_QUOTES) ?></h1>
    <p class="lead">This is your dashboard. Use the menu above to navigate.</p>

    <?php if ($isAdmin): ?>
      <div class="mt-4">
        <h5>Admin Tools</h5>
        <a href="admin/list_users.php" class="btn btn-outline-secondary me-2">Manage Users</a>
        <a href="admin/list_pages.php" class="btn btn-outline-secondary me-2">Manage Pages</a>
        <a href="/app/scheduler/" class="btn btn-outline-primary">Scheduler Dashboard</a>
      </div>
    <?php elseif ($user['role'] === 'scheduler_admin'): ?>
      <div class="mt-4">
        <h5>Scheduler Administration</h5>
        <a href="/app/scheduler/" class="btn btn-outline-primary">Scheduler Dashboard</a>
      </div>
    <?php elseif ($user['role'] === 'editor'): ?>
      <div class="mt-4">
        <a href="admin/list_pages.php" class="btn btn-outline-secondary">Edit Website Pages</a>
      </div>
    <?php elseif ($user['role'] === 'viewer'): ?>
      <div class="alert alert-info mt-4">
        You have viewer access. Use the menu to explore available pages.
      </div>
    <?php endif; ?>

    <div class="mt-5">
      <h5>Available Tools</h5>
      <ul class="list-group">
        <li class="list-group-item">
          <a href="/pages/seo/richsnippet.php">SEO Rich Snippet Generator</a>
        </li>
      </ul>
    </div>

  </div>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
