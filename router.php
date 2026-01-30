<?php
/**
 * PHP Built-in Server Router
 * Replicates Apache .htaccess behavior for local development
 *
 * Usage: php -S localhost:8080 router.php
 */

$uri = urldecode(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH));
$requestedFile = __DIR__ . $uri;

// Block sensitive files
$blockedPatterns = [
    '/\.env$/',
    '/\.(sql|log|md)$/',
    '/^\/sql\//',
    '/\.vault$/',
];

foreach ($blockedPatterns as $pattern) {
    if (preg_match($pattern, $uri)) {
        http_response_code(403);
        echo "Forbidden";
        return true;
    }
}

// If file exists, serve it directly (except PHP which should be executed)
if (is_file($requestedFile)) {
    // Let PHP's built-in server handle PHP files and static assets
    return false;
}

// If directory exists with index.php, serve that
if (is_dir($requestedFile)) {
    $indexFile = rtrim($requestedFile, '/') . '/index.php';
    if (is_file($indexFile)) {
        $_SERVER['SCRIPT_NAME'] = rtrim($uri, '/') . '/index.php';
        include $indexFile;
        return true;
    }
}

// Static assets - return false to let built-in server try
if (preg_match('/\.(js|css|png|jpe?g|gif|ico|svg|woff2?|ttf|eot)$/i', $uri)) {
    return false;
}

// Scheduler app routes - already handled by directory check above
// But handle specific paths
if (preg_match('#^/app/scheduler/#', $uri)) {
    return false; // Let it find the file
}

// Known public scripts
$publicScripts = ['login', 'login_microsoft', 'oauth_callback', 'logout', 'dashboard', 'dev_login', 'index'];
foreach ($publicScripts as $script) {
    if ($uri === "/{$script}.php" || $uri === "/{$script}") {
        $file = __DIR__ . "/{$script}.php";
        if (is_file($file)) {
            include $file;
            return true;
        }
    }
}

// Slug routing - catch-all to index.php
if (preg_match('#^/([a-z0-9\-/]+)/?$#', $uri, $matches)) {
    $_GET['page'] = $matches[1];
    include __DIR__ . '/index.php';
    return true;
}

// Default - let PHP's built-in server handle it
return false;
