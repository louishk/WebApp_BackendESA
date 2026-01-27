<?php
session_start();
require __DIR__ . '/../config.php';
require_role('admin'); // Only admin can manage users

// Fetch users with role
$stmt = $pdo->query("SELECT id, username, email, role, created_at FROM users ORDER BY created_at DESC");
$users = $stmt->fetchAll(PDO::FETCH_ASSOC);
?>
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Manage Users</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="p-4">
  <h1 class="mb-4">Manage Users</h1>
  <a href="register.php" class="btn btn-success mb-3">+ New User</a>
  <a href="admin.php" class="btn btn-secondary mb-3 ms-2">← Back to Admin</a>

  <table class="table table-striped table-bordered">
    <thead class="table-dark">
      <tr>
        <th>#</th>
        <th>Username</th>
        <th>Email</th>
        <th>Role</th>
        <th>Created</th>
        <th>Actions</th>
      </tr>
    </thead>
    <tbody>
      <?php foreach ($users as $u): ?>
        <tr>
          <td><?= htmlspecialchars($u['id']) ?></td>
          <td><?= htmlspecialchars($u['username']) ?></td>
          <td><?= htmlspecialchars($u['email']) ?></td>
          <td><?= htmlspecialchars($u['role']) ?></td>
          <td><?= htmlspecialchars($u['created_at']) ?></td>
          <td>
            <a href="edit_user.php?id=<?= $u['id'] ?>" class="btn btn-sm btn-primary">Edit</a>
            <a href="reset_password.php?id=<?= $u['id'] ?>" class="btn btn-sm btn-warning">Reset PW</a>
            <a href="delete_user.php?id=<?= $u['id'] ?>" class="btn btn-sm btn-danger" onclick="return confirm('Delete user <?= addslashes($u['username']) ?>?');">Delete</a>
          </td>
        </tr>
      <?php endforeach; ?>
    </tbody>
  </table>
</body>
</html>