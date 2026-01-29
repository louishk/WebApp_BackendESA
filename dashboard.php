<?php
require 'config.php';

if (!isset($_SESSION['user'])) {
    header('Location: index.php');
    exit;
}

$user = $_SESSION['user'];
?>
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Dashboard</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <nav class="navbar navbar-expand-lg navbar-light bg-white shadow-sm">
    <div class="container">
      <a class="navbar-brand" href="dashboard.php">ESA Backend</a>
      <div class="collapse navbar-collapse">
        <ul class="navbar-nav ms-auto">
          <?php if (in_array($user['role'], ['admin', 'scheduler_admin'])): ?>
            <li class="nav-item">
              <a class="nav-link" href="/app/scheduler/">Scheduler</a>
            </li>
          <?php endif; ?>
          <?php if ($user['role'] === 'admin'): ?>
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
    <h1>Welcome, <?= h($user['username'] ?? $user['email']) ?></h1>
    <p class="lead">Role: <span class="badge bg-secondary"><?= h($user['role']) ?></span></p>

    <div class="row mt-4">
      <?php if (in_array($user['role'], ['admin', 'scheduler_admin'])): ?>
        <div class="col-md-4 mb-3">
          <div class="card h-100">
            <div class="card-body">
              <h5 class="card-title">PBI Scheduler</h5>
              <p class="card-text">Manage data pipelines and scheduled jobs.</p>
              <a href="/app/scheduler/" class="btn btn-primary">Open Scheduler</a>
            </div>
          </div>
        </div>
      <?php endif; ?>

      <?php if ($user['role'] === 'admin'): ?>
        <div class="col-md-4 mb-3">
          <div class="card h-100">
            <div class="card-body">
              <h5 class="card-title">User Management</h5>
              <p class="card-text">Manage users, roles, and permissions.</p>
              <a href="admin/list_users.php" class="btn btn-outline-secondary">Manage Users</a>
            </div>
          </div>
        </div>
        <div class="col-md-4 mb-3">
          <div class="card h-100">
            <div class="card-body">
              <h5 class="card-title">Page Management</h5>
              <p class="card-text">Manage website pages and content.</p>
              <a href="admin/list_pages.php" class="btn btn-outline-secondary">Manage Pages</a>
            </div>
          </div>
        </div>
      <?php endif; ?>

      <?php if (in_array($user['role'], ['admin', 'editor'])): ?>
        <div class="col-md-4 mb-3">
          <div class="card h-100">
            <div class="card-body">
              <h5 class="card-title">SEO Tools</h5>
              <p class="card-text">Schema markup generator for SEO.</p>
              <a href="pages/seo/richsnippet.php" class="btn btn-outline-secondary">Open SEO Tools</a>
            </div>
          </div>
        </div>
      <?php endif; ?>
    </div>

  </div>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
