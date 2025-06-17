<?php
// DEBUG: show all errors (remove these three lines in production)
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

session_start();
require __DIR__ . '/../config.php';
require_role('admin'); // Admin-only

$id = isset($_GET['id']) ? (int)$_GET['id'] : 0;
if ($id <= 0) {
    die("Invalid user ID");
}

try {
    $stmt = $pdo->prepare("SELECT id, username FROM users WHERE id = ?");
    $stmt->execute([$id]);
    $user = $stmt->fetch(PDO::FETCH_ASSOC);
    if (!$user) {
        die("User not found.");
    }
} catch (PDOException $e) {
    die("DB Error (fetch): " . htmlspecialchars($e->getMessage()));
}

$errors = [];
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $password = $_POST['password'] ?? '';
    $confirm  = $_POST['confirm_password'] ?? '';

    if (strlen($password) < 6) {
        $errors[] = "Password must be at least 6 characters.";
    }
    if ($password !== $confirm) {
        $errors[] = "Password and confirmation do not match.";
    }

    if (empty($errors)) {
        $hash = password_hash($password, PASSWORD_DEFAULT);
        try {
            $pdo->prepare("UPDATE users SET password = ? WHERE id = ?")
                ->execute([$hash, $id]);
            header('Location: list_users.php');
            exit;
        } catch (PDOException $e) {
            die("DB Error (update): " . htmlspecialchars($e->getMessage()));
        }
    }
}
?>
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Reset Password: <?= htmlspecialchars($user['username'], ENT_QUOTES) ?></title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="p-4">
  <h1 class="mb-4">Reset Password for <?= htmlspecialchars($user['username'], ENT_QUOTES) ?></h1>

  <?php if ($errors): ?>
    <div class="alert alert-danger"><ul>
      <?php foreach ($errors as $e): ?>
        <li><?= htmlspecialchars($e, ENT_QUOTES) ?></li>
      <?php endforeach; ?>
    </ul></div>
  <?php endif; ?>

  <form method="post" novalidate>
    <div class="mb-3">
      <label class="form-label">New Password</label>
      <input type="password" name="password" class="form-control" required>
    </div>
    <div class="mb-3">
      <label class="form-label">Confirm Password</label>
      <input type="password" name="confirm_password" class="form-control" required>
    </div>
    <button class="btn btn-warning">Reset Password</button>
    <a href="list_users.php" class="btn btn-secondary ms-2">Cancel</a>
  </form>
</body>
</html>