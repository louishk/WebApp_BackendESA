<?php
session_start();
require __DIR__ . '/../config.php';
require_role('admin'); // Admin-only access

// Fetch some stats
$userCount = (int)$pdo->query("SELECT COUNT(*) FROM users")->fetchColumn();
$pageCount = (int)$pdo->query("SELECT COUNT(*) FROM pages")->fetchColumn();
?>
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Admin Panel</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <nav class="navbar navbar-expand-lg navbar-light bg-white shadow-sm">
    <div class="container">
      <a class="navbar-brand" href="../dashboard.php">MyApp</a>
      <ul class="navbar-nav ms-auto">
        <li class="nav-item"><a class="nav-link" href="../logout.php">Logout</a></li>
      </ul>
    </div>
  </nav>

  <div class="container py-5">
    <h1 class="mb-4">Admin Dashboard</h1>
    <div class="row g-4">
      <div class="col-md-4">
        <div class="card text-center">
          <div class="card-body">
            <h2><?= $userCount ?></h2>
            <p>Total Users</p>
            <a href="list_users.php" class="btn btn-outline-primary">Manage Users</a>
          </div>
        </div>
      </div>
      <div class="col-md-4">
        <div class="card text-center">
          <div class="card-body">
            <h2><?= $pageCount ?></h2>
            <p>Total Pages</p>
            <a href="list_pages.php" class="btn btn-outline-primary">Manage Pages</a>
          </div>
        </div>
      </div>
      <div class="col-md-4">
        <div class="card text-center">
          <div class="card-body">
            <h2>📅</h2>
            <p>Scheduler</p>
            <a href="../app/scheduler/" class="btn btn-outline-primary">Open Scheduler</a>
          </div>
        </div>
      </div>
      <div class="col-md-4">
        <div class="card text-center">
          <div class="card-body">
            <h2>⚙️</h2>
            <p>Settings</p>
            <a href="#" class="btn btn-outline-secondary disabled">TBD</a>
          </div>
        </div>
      </div>
    </div>
  </div>
</body>
</html>
