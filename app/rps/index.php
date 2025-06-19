<?php
/**
 * Enhanced RapidStor Descriptor Manager - Main Entry Point
 * Fixed version with proper include order
 */

session_start();

// Include all required classes FIRST
require_once 'config.php';
require_once 'RapidStorAPI.php';
require_once 'DataLoader.php';
require_once 'AjaxHandler.php';
require_once 'InventoryManager.php';

// Performance and timeout configurations from environment (after config is loaded)
ini_set('max_execution_time', Config::get('MAX_EXECUTION_TIME', 120));
ini_set('memory_limit', Config::get('MEMORY_LIMIT', '256M'));
ini_set('max_input_vars', 5000);

// Disable output buffering for real-time feedback
if (ob_get_level()) {
    ob_end_clean();
}

// Initialize variables with environment-aware defaults
$jwtToken = $_SESSION['jwt_token'] ?? Config::getJwtToken() ?? '';
$message = '';
$messageType = '';
$selectedLocation = $_GET['location'] ?? $_SESSION['location'] ?? Config::getDefaultLocation();
$searchTerm = $_GET['search'] ?? '';
$sortBy = $_GET['sort'] ?? 'ordinalPosition';
$sortOrder = $_GET['order'] ?? 'asc';
$viewMode = $_GET['view'] ?? 'table';
$debug = isset($_GET['debug']) || Config::isDebugMode();

// Handle JWT token input
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['jwt_token'])) {
    $jwtToken = trim($_POST['jwt_token']);
    $_SESSION['jwt_token'] = $jwtToken;
}

// Initialize API and validate location
if (!Config::isValidLocation($selectedLocation)) {
    $selectedLocation = Config::getDefaultLocation();
}
$_SESSION['location'] = $selectedLocation;

$api = new RapidStorAPI($jwtToken, $debug);

// Show environment status if debug mode
if ($debug) {
    $envStatus = [
        'env_file_loaded' => Config::isEnvFileLoaded(),
        'env_file_path' => Config::getEnvFilePath(),
        'api_config' => $api->getConfig(),
        'php_settings' => [
            'memory_limit' => ini_get('memory_limit'),
            'max_execution_time' => ini_get('max_execution_time'),
            'error_reporting' => error_reporting(),
            'display_errors' => ini_get('display_errors')
        ]
    ];

    if (Config::isEnvFileLoaded()) {
        $message = "✅ Environment loaded from: " . Config::getEnvFilePath();
        $messageType = 'success';
    } else {
        $message = "⚠️ No .env file found. Using default configuration.";
        $messageType = 'warning';
    }
}

// ============================================================================
// AJAX REQUEST HANDLING
// ============================================================================

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $action = $_POST['action'] ?? '';

    // List of all AJAX actions that should be handled by AjaxHandler
    $ajaxActions = [
        'quick_toggle',
        'reorder_descriptors',
        'group_descriptors',
        'batch_update',
        'batch_apply',
        'auto_generate_upsells',
        'smart_carousel_off',
        'delete_descriptor',
        'duplicate_descriptor',
        'export_descriptors',
        'get_descriptor'
    ];

    // Handle AJAX requests
    if (in_array($action, $ajaxActions)) {
        try {
            error_log("Processing AJAX action: {$action}");

            $ajaxHandler = new AjaxHandler($api, $selectedLocation, $debug);
            $response = $ajaxHandler->handleRequest($action, $_POST);

            error_log("AJAX response for {$action}: " . json_encode($response));

            // Ensure we're sending JSON
            header('Content-Type: application/json');
            echo json_encode($response);
            exit;
        } catch (Exception $e) {
            error_log("AJAX error for {$action}: " . $e->getMessage());

            // Return error as JSON
            header('Content-Type: application/json');
            echo json_encode([
                'success' => false,
                'error' => $e->getMessage()
            ]);
            exit;
        }
    }
}

// ============================================================================
// REGULAR FORM HANDLING
// ============================================================================

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    try {
        $action = $_POST['action'] ?? '';

        switch ($action) {
            case 'login':
                $result = $api->login($_POST['force_refresh'] ?? false);
                if ($result['status'] === 200) {
                    $message = 'Successfully logged into RapidStor';
                    $messageType = 'success';
                } else {
                    $message = 'Login failed: ' . ($result['data']['error'] ?? 'Unknown error');
                    $messageType = 'error';
                }
                break;

            case 'create_env_file':
                $envPath = '.env';
                if (Config::createSampleEnvFile($envPath)) {
                    $message = "Sample .env file created at {$envPath}. Please edit it with your settings.";
                    $messageType = 'success';
                } else {
                    $message = "Failed to create .env file. Check permissions.";
                    $messageType = 'error';
                }
                break;

            case 'save_descriptor':
                error_log("► HIT save_descriptor");
                $descriptorData = [
                    'name' => $_POST['name'],
                    'description' => $_POST['description'],
                    'specialText' => $_POST['specialText'] ?? '',
                    'ordinalPosition' => (int)($_POST['ordinalPosition'] ?? 0),
                    'enabled' => isset($_POST['enabled']),
                    'hidden' => !isset($_POST['visible']),
                    'useForCarousel' => isset($_POST['useForCarousel']),
                    'sCorpCode' => 'CNCK',
                    'sLocationCode' => $selectedLocation
                ];

                if (!empty($_POST['_id'])) {
                    // Update existing descriptor
                    $descriptorData['_id'] = $_POST['_id'];

                    $dataLoader = new DataLoader($api, $selectedLocation, $debug);
                    $allData = $dataLoader->loadAllData();
                    $existingDescriptor = null;

                    foreach ($allData['descriptors'] as $desc) {
                        if ($desc['_id'] === $_POST['_id']) {
                            $existingDescriptor = $desc;
                            break;
                        }
                    }

                    if ($existingDescriptor) {
                        $descriptorData = array_merge($existingDescriptor, $descriptorData);
                    }
                } else {
                    // New descriptor - add defaults
                    $descriptorData = array_merge(Config::DEFAULT_DESCRIPTOR, $descriptorData);
                }

                // Handle upsells
                $upsells = [];
                if (isset($_POST['upsells']) && is_array($_POST['upsells'])) {
                    foreach ($_POST['upsells'] as $u) {
                        if (!empty($u['_id']) && !empty($u['upgradeReason'])) {
                            $upsells[] = [
                                '_id' => trim($u['_id']),
                                'upgradeIcon' => 'fa-warehouse',
                                'upgradeIconPrefix' => 'fa-light',
                                'upgradeReason' => trim($u['upgradeReason'])
                            ];
                        }
                    }
                }
                $descriptorData['upgradesTo'] = $upsells;

                $result = $api->saveDescriptor($descriptorData, $selectedLocation);
                if ($result['status'] === 200) {
                    $message = 'Descriptor saved successfully';
                    $messageType = 'success';
                } else {
                    $errorMsg = 'Unknown error';
                    if (isset($result['data']['error'])) {
                        $errorMsg = $result['data']['error'];
                    } elseif (isset($result['data']['message'])) {
                        $errorMsg = $result['data']['message'];
                    } elseif (!empty($result['raw'])) {
                        $errorMsg = "HTTP {$result['status']}: " . substr($result['raw'], 0, 300);
                    }
                    $message = 'Save failed: ' . $errorMsg;
                    $messageType = 'error';
                }
                break;

            case 'save_descriptor_limited':
                // Limited save handling
                if (empty($_POST['_id'])) {
                    $message = 'Descriptor ID is required for limited save';
                    $messageType = 'error';
                    break;
                }

                try {
                    $dataLoader = new DataLoader($api, $selectedLocation, $debug);
                    $allData = $dataLoader->loadAllData();
                    $existingDescriptor = null;
                    foreach ($allData['descriptors'] as $desc) {
                        if ($desc['_id'] === $_POST['_id']) {
                            $existingDescriptor = $desc;
                            break;
                        }
                    }
                    if (!$existingDescriptor) {
                        $message = 'Descriptor not found';
                        $messageType = 'error';
                        break;
                    }

                    // Start with existing data
                    $descriptorData = $existingDescriptor;

                    // Update fields...
                    $keywords = array_filter(array_map('trim', $_POST['keywords'] ?? []), function($k) {
                        return $k !== '';
                    });
                    $descriptorData['criteria']['include']['keywords'] = array_values($keywords);

                    $descriptorData['description'] = trim($_POST['description'] ?? '');
                    $descs = array_filter(array_map('trim', $_POST['descriptions'] ?? []), function($d) {
                        return $d !== '';
                    });
                    $descriptorData['descriptions'] = array_values($descs);
                    $descriptorData['specialText'] = trim($_POST['specialText'] ?? '');

                    $selectedDeals = $_POST['deals'] ?? [];
                    $descriptorData['deals'] = array_values($selectedDeals);

                    $insurance = trim($_POST['defaultInsuranceCoverage'] ?? '');
                    $descriptorData['defaultInsuranceCoverage'] = $insurance !== '' ? $insurance : null;

                    $upsells = [];
                    if (!empty($_POST['upsells']) && is_array($_POST['upsells'])) {
                        foreach ($_POST['upsells'] as $u) {
                            if (!empty($u['_id']) && !empty($u['upgradeReason'])) {
                                $upsells[] = [
                                    '_id' => trim($u['_id']),
                                    'upgradeIcon' => 'fa-warehouse',
                                    'upgradeIconPrefix' => 'fa-light',
                                    'upgradeReason' => trim($u['upgradeReason']),
                                ];
                            }
                        }
                    }
                    $descriptorData['upgradesTo'] = $upsells;

                    // Persist
                    $result = $api->saveDescriptor($descriptorData, $selectedLocation);
                    if ($result['status'] === 200) {
                        // Build change summary
                        $changes = [];
                        if ($keywords !== ($existingDescriptor['criteria']['include']['keywords'] ?? [])) {
                            $changes[] = 'keywords';
                        }
                        if ($descs !== ($existingDescriptor['descriptions'] ?? []) ||
                            $descriptorData['description'] !== ($existingDescriptor['description'] ?? '') ||
                            $descriptorData['specialText'] !== ($existingDescriptor['specialText'] ?? '')) {
                            $changes[] = 'descriptions';
                        }
                        if ($selectedDeals !== ($existingDescriptor['deals'] ?? [])) {
                            $changes[] = 'deals';
                        }
                        if ($descriptorData['defaultInsuranceCoverage'] !== ($existingDescriptor['defaultInsuranceCoverage'] ?? null)) {
                            $changes[] = 'insurance';
                        }
                        if ($upsells !== ($existingDescriptor['upgradesTo'] ?? [])) {
                            $changes[] = 'upsells';
                        }

                        if (count($changes) > 0) {
                            $message = 'Descriptor updated successfully (' . implode(', ', $changes) . ')';
                        } else {
                            $message = 'No changes detected';
                        }
                        $messageType = 'success';

                        // Redirect out of edit mode
                        header("Location: " . strtok($_SERVER["REQUEST_URI"], '?') . "?location={$selectedLocation}");
                        exit;
                    } else {
                        // API error
                        $err = $result['data']['error']
                            ?? $result['data']['message']
                            ?? (!empty($result['raw']) ? "HTTP {$result['status']}: " . substr($result['raw'],0,300) : 'Unknown error');
                        $message = 'Save failed: ' . $err;
                        $messageType = 'error';
                    }
                } catch (Exception $e) {
                    $message = 'Error updating descriptor: ' . $e->getMessage();
                    $messageType = 'error';
                }
                break;

            // Test endpoints
            case 'test_unittypes':
                $dataLoader = new DataLoader($api, $selectedLocation, $debug);
                $result = $dataLoader->testEndpoint("/rapidstor/api/unittypes", ['location' => $selectedLocation]);

                if ($result['success']) {
                    $message = "Unit Types API test successful! Found data. URL: {$result['url']}";
                    $messageType = 'success';
                } else {
                    $message = "Unit Types API test failed: " . ($result['error'] ?? 'Unknown error');
                    $messageType = 'error';
                }
                break;

            case 'test_descriptors':
                $dataLoader = new DataLoader($api, $selectedLocation, $debug);
                $result = $dataLoader->testEndpoint("/rapidstor/api/descriptors", ['location' => $selectedLocation]);

                if ($result['success']) {
                    $message = "Descriptors API test successful! URL: {$result['url']}";
                    $messageType = 'success';
                } else {
                    $message = "Descriptors API test failed: " . ($result['error'] ?? 'Unknown error');
                    $messageType = 'error';
                }
                break;

            case 'test_connection':
                $result = $api->getStatus();
                if ($result['status'] === 200) {
                    $message = 'API connection successful!';
                    $messageType = 'success';
                } else {
                    $message = "API connection failed. Status: {$result['status']}";
                    $messageType = 'error';
                }
                break;

            case 'reload_env':
                // Force reload environment configuration
                $oldToken = Config::getJwtToken();
                $oldBaseUrl = Config::getApiBaseUrl();

                // Clear the loaded flag to force re-read
                try {
                    $reflection = new ReflectionClass('Config');
                    $loadedProperty = $reflection->getProperty('loaded');
                    $loadedProperty->setAccessible(true);
                    $loadedProperty->setValue(false);

                    // Re-initialize API with new config
                    $api = new RapidStorAPI($jwtToken, $debug);

                    $message = "Environment configuration reloaded. ";
                    if ($oldToken !== Config::getJwtToken()) {
                        $message .= "JWT token updated. ";
                    }
                    if ($oldBaseUrl !== Config::getApiBaseUrl()) {
                        $message .= "API URL updated. ";
                    }
                    $messageType = 'success';
                } catch (Exception $e) {
                    $message = "Failed to reload environment: " . $e->getMessage();
                    $messageType = 'error';
                }
                break;
        }
    } catch (Exception $e) {
        $message = 'Error: ' . $e->getMessage();
        $messageType = 'error';
    }
}

// ============================================================================
// DATA LOADING
// ============================================================================

$data = [
    'descriptors' => [],
    'deals' => [],
    'insurance' => [],
    'unitTypes' => [],
    'lookups' => [
        'deals' => [],
        'insurance' => [],
        'unitTypes' => []
    ],
    'stats' => []
];

if (!$api->hasValidToken()) {
    $message = 'Please provide a valid JWT token to access the API.';
    $messageType = 'error';
} else {
    try {
        $dataLoader = new DataLoader($api, $selectedLocation, $debug);
        $data = $dataLoader->loadAllData();

        // Apply search filter
        if ($searchTerm) {
            $data['descriptors'] = $dataLoader->filterDescriptors($data['descriptors'], $searchTerm);
        }

        // Apply sorting
        $data['descriptors'] = $dataLoader->sortDescriptors($data['descriptors'], $sortBy, $sortOrder);

        // Group descriptors if in grouped view
        if ($viewMode === 'grouped') {
            $data['groupedDescriptors'] = $dataLoader->groupDescriptors($data['descriptors']);
        }

    } catch (Exception $e) {
        $message = 'Error loading data: ' . $e->getMessage();
        $messageType = 'error';
    }
}

// Get editing descriptor
$editingDescriptor = null;
if (isset($_GET['edit'])) {
    $editingId = $_GET['edit'];
    $editingDescriptor = array_filter($data['descriptors'], function($desc) use ($editingId) {
        return $desc['_id'] === $editingId;
    });
    $editingDescriptor = reset($editingDescriptor);
}

// Include the view
include 'view.php';
?>