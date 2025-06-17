<?php
require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/config.php';   // calls session_start()

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

// 2) Generate the auth URL (this also generates & stores the state internally)
$authUrl = $provider->getAuthorizationUrl();

// 3) Now grab the newly generated state and persist it
$_SESSION['oauth2state'] = $provider->getState();

// 4) Redirect to Microsoft’s OAuth 2.0 endpoint
header('Location: ' . $authUrl);
exit;
