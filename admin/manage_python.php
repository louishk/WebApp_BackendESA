<?php
/**
 * manage_python.php (Enhanced + Web Terminal)
 *
 * - Upload Python scripts to /python
 * - Edit .env with CodeMirror
 * - Manage crontab with visual dashboard
 * - Run scripts manually and view output in real-time
 */

require __DIR__ . '/../config.php';
session_start();
require_role('admin');

$pythonDir = realpath(__DIR__ . '/../python') ?: __DIR__ . '/../python';
if (!is_dir($pythonDir)) mkdir($pythonDir, 0755, true);
if (!is_writable($pythonDir)) exit('Error: Python directory not writable.');
$venvPython = "$pythonDir/venv/bin/python";
$envFile    = "$pythonDir/.env";
$logDir     = sys_get_temp_dir();

// CSRF token setup
if (empty($_SESSION['csrf_token'])) {
  $_SESSION['csrf_token'] = bin2hex(random_bytes(32));
}

function getCrontabLines() {
  exec('crontab -l 2>/dev/null', $lines, $st);
  return $st === 0 ? $lines : [];
}

function writeCrontabLines($lines) {
  $file = tempnam(sys_get_temp_dir(), 'cron');
  file_put_contents($file, implode("\n", $lines) . "\n");
  exec("crontab $file");
  unlink($file);
}

function validateCSRF() {
  return hash_equals($_SESSION['csrf_token'], $_POST['csrf_token'] ?? '');
}

$message = '';

// Upload handler
if (!empty($_FILES['script_upload'])) {
  $f = $_FILES['script_upload'];
  if ($f['error'] === UPLOAD_ERR_OK && preg_match('/\.py$/', $f['name'])) {
    $dest = "$pythonDir/" . basename($f['name']);
    if (move_uploaded_file($f['tmp_name'], $dest)) {
      $message = "Uploaded to $dest";
    } else {
      $message = "Upload failed.";
    }
  } else {
    $message = "Invalid file.";
  }
}

// Save .env
if (isset($_POST['env_contents']) && validateCSRF()) {
  if (file_put_contents($envFile, trim($_POST['env_contents']) . "\n") !== false) {
    $message = ".env saved.";
  } else {
    $message = "Could not save .env";
  }
}

// Handle cron
if (!empty($_POST['cron_script']) && validateCSRF()) {
  $script = preg_replace('/[^a-zA-Z0-9_\-.]/', '', basename($_POST['cron_script']));
  $freq = $_POST['frequency'] ?? 'custom';
  switch ($freq) {
    case 'hourly':  $m = $_POST['minute'] ?? 0; $expr = "$m * * * *"; break;
    case 'daily':   $m = $_POST['minute'] ?? 0; $h = $_POST['hour'] ?? 0; $expr = "$m $h * * *"; break;
    case 'weekly':  $m = $_POST['minute'] ?? 0; $h = $_POST['hour'] ?? 0; $wd = $_POST['weekday'] ?? 0; $expr = "$m $h * * $wd"; break;
    case 'monthly': $m = $_POST['minute'] ?? 0; $h = $_POST['hour'] ?? 0; $dom = $_POST['monthday'] ?? 1; $expr = "$m $h $dom * *"; break;
    default: $expr = trim($_POST['custom_expr'] ?? '');
  }
  if (!preg_match('/^(\S+\s+){4}\S+$/', $expr)) {
    $message = "Invalid cron expression.";
  } else {
    $lines = getCrontabLines();
    $lines = array_filter($lines, fn($l) => strpos($l, "$venvPython $pythonDir/$script") === false);
    $lines[] = "$expr cd $pythonDir && $venvPython $script >> $pythonDir/python.log 2>&1";
    writeCrontabLines($lines);
    $message = "Schedule updated.";
  }
}

// Delete script
if (!empty($_POST['delete_script']) && validateCSRF()) {
  $script = preg_replace('/[^a-zA-Z0-9_\-.]/', '', basename($_POST['delete_script']));
  $path = "$pythonDir/$script";
  if (file_exists($path)) {
    unlink($path);
    $message = "Deleted $script.";
  }
}

// Run Now => async redirect to terminal.php
if (!empty($_POST['run_script']) && validateCSRF()) {
  $script = preg_replace('/[^a-zA-Z0-9_\-.]/', '', basename($_POST['run_script']));
  header("Location: terminal.php?script=" . urlencode($script));
  exit;
}

$scripts = array_map('basename', glob("$pythonDir/*.py"));
$envContent = file_exists($envFile) ? file_get_contents($envFile) : '';
$cronLines = getCrontabLines();
$scheduled = [];
foreach ($cronLines as $l) {
  if (preg_match('/^([\d\* ,]+) cd .*python && .*python ([^\s]+)/', $l, $m)) {
    $scheduled[$m[2]] = $m[1];
  }
}
?>
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Manage Python</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
  <link href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/codemirror.min.css" rel="stylesheet">
  <style>
    .CodeMirror { border: 1px solid #ddd; height: auto; }
  </style>
</head>
<body>
<div class="container mt-5">
  <h1>Python Script Manager</h1>
  <?php if($message): ?><div class="alert alert-info"><?=htmlspecialchars($message)?></div><?php endif; ?>
  <ul class="nav nav-tabs" role="tablist">
    <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#upload">Upload</button></li>
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#env">.env</button></li>
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#scripts">Scripts</button></li>
  </ul>

  <div class="tab-content p-4 border bg-white">
    <div class="tab-pane fade show active" id="upload">
      <form method="post" enctype="multipart/form-data">
        <input type="file" name="script_upload" accept=".py" class="form-control mb-3" required>
        <input type="hidden" name="csrf_token" value="<?=$_SESSION['csrf_token']?>">
        <button class="btn btn-primary">Upload</button>
      </form>
    </div>

    <div class="tab-pane fade" id="env">
      <form method="post">
        <textarea id="envContents" name="env_contents"><?=htmlspecialchars($envContent)?></textarea>
        <input type="hidden" name="csrf_token" value="<?=$_SESSION['csrf_token']?>">
        <button class="btn btn-primary mt-2">Save .env</button>
      </form>
    </div>

    <div class="tab-pane fade" id="scripts">
      <?php foreach ($scripts as $script): ?>
        <div class="border rounded p-3 mb-4">
          <h5 class="mb-3"><?=htmlspecialchars($script)?></h5>

          <form method="post" class="d-inline">
            <input type="hidden" name="run_script" value="<?=htmlspecialchars($script)?>">
            <input type="hidden" name="csrf_token" value="<?=$_SESSION['csrf_token']?>">
            <button class="btn btn-success btn-sm">Run Now</button>
          </form>

          <form method="post" class="d-inline ms-2" onsubmit="return confirm('Delete this script?');">
            <input type="hidden" name="delete_script" value="<?=htmlspecialchars($script)?>">
            <input type="hidden" name="csrf_token" value="<?=$_SESSION['csrf_token']?>">
            <button class="btn btn-danger btn-sm">Delete</button>
          </form>

          <form class="row row-cols-lg-auto g-3 align-items-center mt-3" method="post">
            <input type="hidden" name="cron_script" value="<?=htmlspecialchars($script)?>">
            <input type="hidden" name="csrf_token" value="<?=$_SESSION['csrf_token']?>">

            <div class="col">
              <label class="form-label">Frequency</label>
              <select name="frequency" class="form-select freq-select">
                <option value="hourly">Hourly</option>
                <option value="daily">Daily</option>
                <option value="weekly">Weekly</option>
                <option value="monthly">Monthly</option>
                <option value="custom">Custom</option>
              </select>
            </div>

            <div class="col time-inputs">
              <label class="form-label">Time</label>
              <input type="time" name="time" class="form-control time-field">
            </div>

            <div class="col custom-input" style="display:none;">
              <label class="form-label">Cron Expr</label>
              <input type="text" name="custom_expr" class="form-control" placeholder="* * * * *"
                value="<?=htmlspecialchars($scheduled[$script] ?? '')?>">
            </div>

            <div class="col">
              <button class="btn btn-primary mt-4">Save</button>
            </div>
          </form>
        </div>
      <?php endforeach; ?>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/js/bootstrap.bundle.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/codemirror.min.js"></script>
<script>
  CodeMirror.fromTextArea(document.getElementById('envContents'), {
    mode: 'properties',
    lineNumbers: true
  });

  document.querySelectorAll('.freq-select').forEach(function(sel){
    sel.addEventListener('change', function() {
      var form = sel.closest('form');
      var ti = form.querySelector('.time-inputs');
      var ci = form.querySelector('.custom-input');
      if (sel.value === 'custom') {
        ti.style.display = 'none';
        ci.style.display = 'block';
      } else {
        ti.style.display = 'block';
        ci.style.display = 'none';
      }
    });
  });
</script>
</body>
</html>
