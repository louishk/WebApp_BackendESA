<?php
session_start();
require __DIR__ . '/../config.php';
require_role('admin');

$rootDir    = dirname(__DIR__);
$pythonDir  = $rootDir . '/python/scripts';
$venvPython = $rootDir . '/python/venv/bin/python';

$script = $_GET['script'] ?? '';
if (!preg_match('/^[a-zA-Z0-9_.\- ]+\.py$/', $script)) {
    http_response_code(400);
    exit("Invalid script name format.");
}

$scriptPath = realpath("$pythonDir/$script");
if (!$scriptPath || !file_exists($scriptPath) || strpos($scriptPath, $pythonDir) !== 0) {
    http_response_code(400);
    exit("Script not found or invalid path.");
}

$logFile = sys_get_temp_dir() . '/term_' . md5($script . session_id()) . '.log';
file_put_contents($logFile, '');

$cmd = "cd $pythonDir && $venvPython $script > " . escapeshellarg($logFile) . " 2>&1 & echo $!";
$pid = shell_exec($cmd);
?>
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Running <?=htmlspecialchars($script)?></title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body { background: #1e1e1e; color: #ccc; }
    .console { background: #000; color: #0f0; font-family: monospace; white-space: pre-wrap; padding: 1em; height: 500px; overflow-y: auto; }
  </style>
</head>
<body>
<div class="container py-4">
  <h2 class="text-light mb-3">Running: <?=htmlspecialchars($script)?></h2>
  <div class="console" id="console">Launching script...</div>
  <a href="manage_python.php" class="btn btn-outline-light mt-3">Back</a>
</div>

<script>
const logFile = <?=json_encode(basename($logFile))?>;
const consoleBox = document.getElementById('console');

function fetchLog() {
  fetch("terminal_stream.php?log=" + logFile)
    .then(r => r.text())
    .then(text => {
      consoleBox.textContent = text;
      consoleBox.scrollTop = consoleBox.scrollHeight;
    });
}

setInterval(fetchLog, 500);
fetchLog();
</script>
</body>
</html>