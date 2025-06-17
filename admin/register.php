<?php
session_start();
require __DIR__ . '/../config.php';
require_role('admin'); // Admin-only access

$errors   = [];
$username = '';
$email    = '';
$role     = 'viewer';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $username = trim($_POST['username'] ?? '');
    $email    = trim($_POST['email'] ?? '');
    $password = $_POST['password'] ?? '';
    $confirm  = $_POST['confirm_password'] ?? '';
    $role     = in_array($_POST['role'], ['admin', 'editor', 'viewer']) ? $_POST['role'] : 'viewer';

    if ($username === '') {
        $errors[] = "Username is required.";
    }
    if (!filter_var($email, FILTER_VALIDATE_EMAIL)) {
        $errors[] = "A valid email is required.";
    }
    if (strlen($password) < 6) {
        $errors[] = "Password must be at least 6 characters.";
    }
    if ($password !== $confirm) {
        $errors[] = "Password and confirmation do not match.";
    }

    if (!$errors) {
        $stmt = $pdo->prepare("SELECT COUNT(*) FROM users WHERE username = ? OR email = ?");
        $stmt->execute([$username, $email]);
        if ($stmt->fetchColumn() > 0) {
            $errors[] = "Username or email already in use.";
        }
    }

    if (!$errors) {
        $hash = password_hash($password, PASSWORD_DEFAULT);
        $pdo->prepare(
            "INSERT INTO users (username, email, password, role, created_at) VALUES (?, ?, ?, ?, NOW())"
        )->execute([$username, $email, $hash, $role]);

        header('Location: list_users.php');
        exit;
    }
}
?>
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>New User</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="p-4">
  <h1 class="mb-4">Create New User</h1>
  <?php if ($errors): ?>
    <div class="alert alert-danger"><ul>
      <?php foreach ($errors as $e): ?>
        <li><?= htmlspecialchars($e, ENT_QUOTES) ?></li>
      <?php endforeach; ?>
    </ul></div>
  <?php endif; ?>
  <form method="post" novalidate>
    <div class="mb-3">
      <label class="form-label">Username</label>
      <input type="text" name="username" class="form-control" required value="<?= htmlspecialchars($username, ENT_QUOTES) ?>">
    </div>
    <div class="mb-3">
      <label class="form-label">Email</label>
      <input type="email" name="email" class="form-control" required value="<?= htmlspecialchars($email, ENT_QUOTES) ?>">
    </div>
    <div class="mb-3">
      <label class="form-label">Password</label>
      <input type="password" name="password" class="form-control" required>
    </div>
    <div class="mb-3">
      <label class="form-label">Confirm Password</label>
      <input type="password" name="confirm_password" class="form-control" required>
    </div>
    <div class="mb-3">
      <label class="form-label">Role</label>
      <select name="role" class="form-select" required>
        <?php foreach (["admin", "editor", "viewer"] as $r): ?>
          <option value="<?= $r ?>" <?= $role === $r ? 'selected' : '' ?>><?= ucfirst($r) ?></option>
        <?php endforeach; ?>
      </select>
    </div>
    <button class="btn btn-primary">Create User</button>
    <a href="list_users.php" class="btn btn-secondary ms-2">Cancel</a>
  </form>
</body>
</html>