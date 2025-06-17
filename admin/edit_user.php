<?php
// DEBUG: turn on error display
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

session_start();
require __DIR__ . '/../config.php';
require_role('admin'); // RBAC admin-only

$id = isset($_GET['id']) ? (int)$_GET['id'] : 0;
if ($id <= 0) {
    die("Invalid user ID");
}

try {
    $stmt = $pdo->prepare("SELECT id, username, email, role FROM users WHERE id = ?");
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
    $email    = trim($_POST['email'] ?? '');
    $password = trim($_POST['password'] ?? '');
    $role     = in_array($_POST['role'], ['admin', 'editor', 'viewer']) ? $_POST['role'] : 'viewer';

    if (!filter_var($email, FILTER_VALIDATE_EMAIL)) {
        $errors[] = "A valid email is required.";
    }

    if (empty($errors)) {
        $sql    = "UPDATE users SET email = ?, role = ?";
        $params = [$email, $role];

        if ($password !== '') {
            $sql      .= ", password = ?";
            $params[] = password_hash($password, PASSWORD_DEFAULT);
        }
        $sql     .= " WHERE id = ?";
        $params[] = $id;

        try {
            $pdo->prepare($sql)->execute($params);
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
  <title>Edit User: <?= htmlspecialchars($user['username'], ENT_QUOTES) ?></title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="p-4">
  <h1 class="mb-4">Edit User: <?= htmlspecialchars($user['username'], ENT_QUOTES) ?></h1>
  <?php if ($errors): ?>
    <div class="alert alert-danger"><ul>
      <?php foreach ($errors as $e): ?>
        <li><?= htmlspecialchars($e, ENT_QUOTES) ?></li>
      <?php endforeach; ?>
    </ul></div>
  <?php endif; ?>

  <form method="post" novalidate>
    <div class="mb-3">
      <label class="form-label">Email</label>
      <input type="email" name="email" class="form-control" required value="<?= htmlspecialchars($user['email'] ?? '', ENT_QUOTES) ?>">
    </div>
    <div class="mb-3">
      <label class="form-label">New Password <small>(leave blank to keep current)</small></label>
      <input type="password" name="password" class="form-control">
    </div>
    <div class="mb-3">
      <label class="form-label">Role</label>
      <select name="role" class="form-select" required>
        <?php foreach (["admin", "editor", "viewer"] as $r): ?>
          <option value="<?= $r ?>" <?= $user['role'] === $r ? 'selected' : '' ?>><?= ucfirst($r) ?></option>
        <?php endforeach; ?>
      </select>
    </div>
    <button class="btn btn-primary">Save Changes</button>
    <a href="list_users.php" class="btn btn-secondary ms-2">Cancel</a>
  </form>
</body>
</html>