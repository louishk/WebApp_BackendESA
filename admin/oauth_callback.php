<?php
require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/config.php';
session_start();

ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

use TheNetworg\OAuth2\Client\Provider\Azure;

$provider = new Azure($azureConfig);
$provider->defaultEndPointVersion = Azure::ENDPOINT_VERSION_2_0;
$provider->scope = [
    'openid',
    'profile',
    'email',
    'offline_access',
    'https://graph.microsoft.com/User.Read'
];

if (!isset($_GET['state']) || $_GET['state'] !== ($_SESSION['oauth2state'] ?? '')) {
    unset($_SESSION['oauth2state']);
    exit('Invalid OAuth state.');
}

try {
    $token = $provider->getAccessToken('authorization_code', [
        'code' => $_GET['code']
    ]);

    $user = $provider->getResourceOwner($token);
    $info = $user->toArray();
    echo '<pre>';
    print_r($info);
    echo '</pre>';
    exit;
    $email = $info['mail'] ?? $info['userPrincipalName'] ?? null;
    $name  = $info['displayName'] ?? 'Microsoft User';

    if (!$email) {
        exit('Unable to retrieve user email from Microsoft.');
    }

    // Check if user exists
    $stmt = $pdo->prepare("SELECT id, username, role FROM users WHERE email = ?");
    $stmt->execute([$email]);
    $dbUser = $stmt->fetch();

    if ($dbUser) {
        $userId   = $dbUser['id'];
        $username = $dbUser['username'];
        $role     = $dbUser['role'];
    } else {
        // Auto-create user with placeholder values
        $username = explode('@', $email)[0]; // "john.doe"
        $password = password_hash('oauth-microsoft', PASSWORD_DEFAULT);
        $role     = 'viewer';

        $stmt = $pdo->prepare("INSERT INTO users (username, email, password, role) VALUES (?, ?, ?, ?)");
        $stmt->execute([$username, $email, $password, $role]);
        $userId = $pdo->lastInsertId();
    }

    // Create session
    $_SESSION['user'] = [
        'id'       => $userId,
        'email'    => $email,
        'name'     => $name,
        'username' => $username,
        'role'     => $role,
        'auth'     => 'microsoft',
    ];

    header('Location: dashboard.php');
    exit;

} catch (\Exception $e) {
    exit('OAuth error: ' . $e->getMessage());
}
