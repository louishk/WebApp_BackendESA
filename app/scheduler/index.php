<?php
/**
 * Scheduler Dashboard Entry Point
 * Requires admin or scheduler_admin role
 */
require_once __DIR__ . '/../../config.php';
require_role(['admin', 'scheduler_admin']);

// Generate or retrieve scheduler token
$schedulerToken = $_SESSION['scheduler_token'] ?? generateSchedulerToken($_SESSION['user']);
$_SESSION['scheduler_token'] = $schedulerToken;

$schedulerApiUrl = $_ENV['SCHEDULER_API_URL'] ?? 'http://localhost:5000';
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PBI Scheduler Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        .scheduler-frame {
            width: 100%;
            height: calc(100vh - 60px);
            border: none;
        }
        .navbar-scheduler {
            background: #2c3e50;
        }
        .navbar-scheduler .navbar-brand,
        .navbar-scheduler .nav-link {
            color: #fff;
        }
        .navbar-scheduler .nav-link:hover {
            color: #ecf0f1;
        }
    </style>
    <script>
        // Make token and API URL available to scheduler UI
        window.SCHEDULER_TOKEN = '<?= htmlspecialchars($schedulerToken, ENT_QUOTES) ?>';
        window.SCHEDULER_API = '/app/scheduler/api/proxy.php?path=';
    </script>
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-scheduler">
        <div class="container-fluid">
            <a class="navbar-brand" href="#">PBI Scheduler</a>
            <ul class="navbar-nav ms-auto">
                <li class="nav-item">
                    <span class="nav-link">
                        Logged in as: <?= htmlspecialchars($_SESSION['user']['username'], ENT_QUOTES) ?>
                        (<?= htmlspecialchars($_SESSION['user']['role'], ENT_QUOTES) ?>)
                    </span>
                </li>
                <li class="nav-item">
                    <a class="nav-link" href="/dashboard.php">Back to Dashboard</a>
                </li>
                <li class="nav-item">
                    <a class="nav-link" href="/logout.php">Logout</a>
                </li>
            </ul>
        </div>
    </nav>

    <div class="container-fluid p-0">
        <iframe id="scheduler-frame" class="scheduler-frame" src="<?= htmlspecialchars($schedulerApiUrl, ENT_QUOTES) ?>"></iframe>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        // Pass authentication token to iframe if same origin, otherwise use postMessage
        const frame = document.getElementById('scheduler-frame');
        frame.addEventListener('load', function() {
            try {
                // Try direct access (same origin)
                if (frame.contentWindow.setAuthToken) {
                    frame.contentWindow.setAuthToken(window.SCHEDULER_TOKEN);
                }
            } catch (e) {
                // Cross-origin, use postMessage
                frame.contentWindow.postMessage({
                    type: 'AUTH_TOKEN',
                    token: window.SCHEDULER_TOKEN
                }, '*');
            }
        });
    </script>
</body>
</html>
