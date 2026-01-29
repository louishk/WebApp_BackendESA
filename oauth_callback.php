<?php
require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/config.php';  // this file already calls session_start()

// — Enable errors for debugging (remove in production) —
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

use TheNetworg\OAuth2\Client\Provider\Azure;

// 1) Instantiate the provider
$provider = new Azure($azureConfig);
$provider->defaultEndPointVersion = Azure::ENDPOINT_VERSION_2_0;
$provider->scope = [
    'openid',
    'profile',
    'email',
    'offline_access',
    'https://graph.microsoft.com/User.Read'
];

// 2) Validate OAuth state to prevent CSRF
if (!isset($_GET['state']) || ($_GET['state'] ?? '') !== ($_SESSION['oauth2state'] ?? '')) {
    unset($_SESSION['oauth2state']);
    exit('Invalid OAuth state.');
}

// 3) Exchange the authorization code for an access token
try {
    $token = $provider->getAccessToken('authorization_code', [
        'code' => $_GET['code']
    ]);

    // 4) Fetch the user’s profile from Microsoft Graph
    $request  = $provider->getAuthenticatedRequest('GET', 'https://graph.microsoft.com/v1.0/me', $token);
    $response = $provider->getHttpClient()->send($request)->getBody()->getContents();
    $userInfo = json_decode($response, true);

    // 5) Extract a reliable email and display name
    $email = $userInfo['mail'] ?? $userInfo['userPrincipalName'] ?? null;
    $name  = $userInfo['displayName'] ?? 'Microsoft User';

    if (!$email) {
        exit('Unable to determine user email.');
    }

    // 6) Look up or auto-create the user
    $stmt = $pdo->prepare("SELECT id, username, role FROM users WHERE email = ?");
    $stmt->execute([$email]);
    $dbUser = $stmt->fetch();

    if ($dbUser) {
        $userId   = $dbUser['id'];
        $username = $dbUser['username'];
        $role     = $dbUser['role'];
    } else {
        $username = explode('@', $email)[0];
        $password = password_hash('oauth-microsoft', PASSWORD_DEFAULT);
        $role     = 'viewer';

        $stmt = $pdo->prepare("INSERT INTO users (username, email, password, role) VALUES (?, ?, ?, ?)");
        $stmt->execute([$username, $email, $password, $role]);
        $userId = $pdo->lastInsertId();
    }

    // 7) Create the session and log the user in
    $_SESSION['user'] = [
        'id'       => $userId,
        'email'    => $email,
        'name'     => $name,
        'username' => $username,
        'role'     => $role,
        'auth'     => 'microsoft',
    ];

    // 8) Generate scheduler token for admin/scheduler_admin
    if (in_array($role, ['admin', 'scheduler_admin'])) {
        $_SESSION['scheduler_token'] = generateSchedulerToken($_SESSION['user']);
    }

    // 9) Redirect to the dashboard
    header('Location: dashboard.php');
    exit;

} catch (\Exception $e) {
    exit('OAuth error: ' . $e->getMessage());
}
