<?php
session_start();
require __DIR__ . '/config.php';


// ─── Page Access Handling ───────────────────────────────────────
if (isset($_GET['page'])) {
    $slug = trim(preg_replace('/[^a-z0-9\-\/]/', '', strtolower($_GET['page'])), '/');

    $stmt = $pdo->prepare("SELECT is_secure FROM pages WHERE slug = ?");
    $stmt->execute([$slug]);
    $rec = $stmt->fetch(PDO::FETCH_ASSOC);

    if ($rec) {
        if ($rec['is_secure'] && empty($_SESSION['user'])) {
            http_response_code(403);
            echo <<<HTML
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Access Restricted</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="container py-5">
  <div class="text-center">
    <div class="alert alert-warning">
      <h1 class="display-6">🔒 Access Restricted</h1>
      <p class="lead">You must log in to view this page.</p>
    </div>
  </div>
</body>
</html>
HTML;
            exit;
        }

        $pageFile = __DIR__ . "/pages/{$slug}.php";
        if (is_file($pageFile)) {
            include $pageFile;
            exit;
        }
    }

    http_response_code(404);
    echo <<<HTML
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>404 Not Found</title></head>
<body style="font-family:sans-serif;text-align:center;padding:2rem;">
  <h1>404: Page Not Found</h1>
  <p>The page "<strong>{$slug}</strong>" does not exist.</p>
  <p><a href="index.php">Back to Home</a></p>
</body>
</html>
HTML;
    exit;
}

// ─── Already Logged In? Go to Dashboard ────────────────────────
if (!empty($_SESSION['user'])) {
    header('Location: dashboard.php');
    exit;
}
?>

<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Login</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <div class="container mt-5">
    <div class="row justify-content-center">
      <div class="col-md-4">
        <div class="card shadow-sm">
          <div class="card-body">
            <h2 class="card-title text-center mb-4">Login</h2>

            <!-- Username/Password Login -->
            <form method="post" action="login.php">
              <div class="mb-3">
                <label for="username" class="form-label">Username</label>
                <input type="text" id="username" name="username" class="form-control" required autofocus>
              </div>
              <div class="mb-3">
                <label for="password" class="form-label">Password</label>
                <input type="password" id="password" name="password" class="form-control" required>
              </div>
              <button type="submit" class="btn btn-primary w-100 mb-2">Login</button>
            </form>

            <!-- Divider -->
            <div class="text-center text-muted mb-2">or</div>

            <!-- Microsoft OAuth Button -->
            <a href="login_microsoft.php" class="btn btn-outline-secondary w-100">
              <img src="https://img.icons8.com/color/16/000000/microsoft.png" class="me-1" />
              Sign in with Microsoft
            </a>

          </div>
        </div>
      </div>
    </div>
  </div>
</body>
</html>
