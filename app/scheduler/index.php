<?php
/**
 * Scheduler App Entry Point
 * Proxies Flask scheduler UI through PHP with authentication
 */

require_once __DIR__ . '/../../config.php';

// Require admin or scheduler_admin role
require_role(['admin', 'scheduler_admin']);

// Get or generate scheduler token
$schedulerToken = $_SESSION['scheduler_token'] ?? null;
if (!$schedulerToken) {
    $schedulerToken = generateSchedulerToken($_SESSION['user']);
    $_SESSION['scheduler_token'] = $schedulerToken;
}

// Scheduler API URL (backend connection - localhost is valid here)
$schedulerUrl = $GLOBALS['schedulerApiUrl'] ?? 'http://localhost:5000';

// Determine which page to proxy
$page = $_GET['page'] ?? '';

// Map routes to Flask endpoints
$targetUrl = match($page) {
    'jobs' => $schedulerUrl . '/jobs',
    'history' => $schedulerUrl . '/history',
    'settings' => $schedulerUrl . '/settings',
    'admin' => $schedulerUrl . '/settings',
    default => $schedulerUrl . '/'
};

// Fetch the Flask page
$ch = curl_init($targetUrl);
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_TIMEOUT => 10,
    CURLOPT_CONNECTTIMEOUT => 5,
    CURLOPT_FOLLOWLOCATION => true,
    CURLOPT_HTTPHEADER => [
        'Accept: text/html',
        'X-Forwarded-For: ' . ($_SERVER['REMOTE_ADDR'] ?? ''),
        'X-Forwarded-User: ' . ($_SESSION['user']['email'] ?? ''),
    ],
]);

$html = curl_exec($ch);
$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$error = curl_error($ch);
curl_close($ch);

// Handle connection errors
if ($error || $httpCode >= 500) {
    // Show error page
    ?>
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Scheduler Unavailable - PBI Scheduler</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: #f5f5f5;
                min-height: 100vh;
                display: flex;
                flex-direction: column;
            }
            .header {
                background: #1a2e5a;
                color: white;
                padding: 1rem 2rem;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            .header h1 { font-size: 1.5rem; }
            .header-nav a {
                color: white;
                text-decoration: none;
                margin-left: 1.5rem;
                opacity: 0.8;
            }
            .header-nav a:hover { opacity: 1; }
            .error-container {
                flex: 1;
                display: flex;
                justify-content: center;
                align-items: center;
                padding: 2rem;
            }
            .error-card {
                background: white;
                border-radius: 8px;
                padding: 3rem;
                text-align: center;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                max-width: 500px;
            }
            .error-card h2 {
                color: #C41230;
                margin-bottom: 1rem;
            }
            .error-card p {
                color: #666;
                margin-bottom: 1.5rem;
            }
            .btn {
                display: inline-block;
                padding: 0.75rem 1.5rem;
                background: #C41230;
                color: white;
                text-decoration: none;
                border-radius: 4px;
                font-weight: 500;
            }
            .btn:hover { background: #9E0E27; }
        </style>
    </head>
    <body>
        <header class="header">
            <h1>PBI Scheduler</h1>
            <nav class="header-nav">
                <a href="/dashboard.php">Dashboard</a>
                <a href="/logout.php">Logout</a>
            </nav>
        </header>
        <div class="error-container">
            <div class="error-card">
                <h2>Scheduler Service Unavailable</h2>
                <p>The scheduler backend is not responding. Please ensure the Python scheduler service is running.</p>
                <p style="font-size: 0.9rem; color: #999;"><?= h($error ?: 'HTTP ' . $httpCode) ?></p>
                <a href="/app/scheduler/" class="btn">Retry</a>
            </div>
        </div>
    </body>
    </html>
    <?php
    exit;
}

// Rewrite URLs in the HTML to use the PHP proxy
// 1a. API calls with leading slash: '/api/* and "/api/* -> /app/scheduler/api/proxy.php?path=*
$html = preg_replace(
    '/([\'"])\/api\//',
    '$1/app/scheduler/api/proxy.php?path=',
    $html
);

// 1b. API calls without leading slash: 'api/* and "api/* -> /app/scheduler/api/proxy.php?path=*
$html = preg_replace(
    '/([\'"])api\//',
    '$1/app/scheduler/api/proxy.php?path=',
    $html
);

// 2. Navigation links: href="./" and href="./jobs" etc -> /app/scheduler/?page=
$html = preg_replace(
    '/href="\.\/([^"]*)"/',
    'href="/app/scheduler/?page=$1"',
    $html
);

// 3. Fix empty page parameter for dashboard (href="/app/scheduler/?page=" -> href="/app/scheduler/")
$html = str_replace('href="/app/scheduler/?page="', 'href="/app/scheduler/"', $html);

// 4. Static assets: src="static/file.ext" -> /app/scheduler/static.php?file=file.ext
$html = preg_replace(
    '/src="static\/([^"]+)"/',
    'src="/app/scheduler/static.php?file=$1"',
    $html
);

// Inject scheduler token for JavaScript API calls
$tokenScript = '<script>window.SCHEDULER_TOKEN = ' . json_encode($schedulerToken) . ';</script>';
$html = str_replace('</head>', $tokenScript . '</head>', $html);

// Inject user info into the navbar
$userName = h($_SESSION['user']['name'] ?? $_SESSION['user']['username'] ?? 'User');
$userRole = h($_SESSION['user']['role'] ?? 'user');
$userInfoHtml = '<span class="user-name">' . $userName . '</span>';
$userInfoHtml .= '<span class="user-role">' . $userRole . '</span>';
$userInfoHtml .= '<a href="/dashboard.php">Dashboard</a>';
$userInfoHtml .= '<a href="/logout.php">Logout</a>';
$html = str_replace('<div class="user-info" id="user-info"></div>', '<div class="user-info" id="user-info">' . $userInfoHtml . '</div>', $html);

// Output the modified HTML
echo $html;
