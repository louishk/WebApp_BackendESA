<?php
/**
 * Enhanced RapidStor Descriptor Manager
 * A web application for managing storage descriptors via API with advanced features
 */

// ============================================================================
// CONFIG.PHP - Configuration Class
// ============================================================================

class Config
{
    const API_BASE_URL = 'https://api.redboxstorage.hk';

    const LOCATIONS = [
        'L004' => 'Location 004',
        'L005' => 'Location 005',
        'L006' => 'Location 006',
        'L007' => 'Location 007',
        'L008' => 'Location 008',
        'L009' => 'Location 009',
        'L010' => 'Location 010'
    ];
}

// ============================================================================
// API.PHP - API Handler Class
// ============================================================================

class RapidStorAPI
{
    private $baseUrl;
    private $token;

    public function __construct($token = null)
    {
        $this->baseUrl = Config::API_BASE_URL;
        $this->token = $token;

        if (empty($this->baseUrl)) {
            throw new Exception("API Base URL not configured.");
        }
    }

    public function setToken($token)
    {
        $this->token = $token;
    }

    public function hasValidToken()
    {
        return !empty($this->token) && $this->token !== 'your_jwt_token_here';
    }

    private function makeRequest($endpoint, $method = 'GET', $data = null)
    {
        if (empty($this->token) && !in_array($endpoint, ['/auth/login', '/status'])) {
            throw new Exception("Authentication required. Please provide a JWT token.");
        }

        $url = $this->baseUrl . $endpoint;

        $headers = [
            'Content-Type: application/json',
            'Accept: application/json',
            'User-Agent: RapidStor-PHP-Client/1.0'
        ];

        if (!empty($this->token)) {
            $headers[] = 'Authorization: Bearer ' . trim($this->token);
        }

        $ch = curl_init();
        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER => $headers,
            CURLOPT_TIMEOUT => 30,
            CURLOPT_CUSTOMREQUEST => $method,
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_VERBOSE => false
        ]);

        if ($data && in_array($method, ['POST', 'PUT', 'PATCH'])) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));
        }

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        $info = curl_getinfo($ch);
        curl_close($ch);

        if ($error) {
            throw new Exception("cURL Error: $error");
        }

        $decoded = json_decode($response, true);

        return [
            'status' => $httpCode,
            'data' => $decoded,
            'raw' => $response,
            'url' => $url,
            'curl_info' => $info,
            'headers_sent' => $headers
        ];
    }

    public function getDescriptors($location = 'L004')
    {
        $endpoint = "/rapidstor/api/descriptors?location=" . urlencode($location);
        return $this->makeRequest($endpoint);
    }

    public function saveDescriptor($descriptorData, $location = 'L004')
    {
        return $this->makeRequest("/rapidstor/api/descriptors/save?location=$location", 'POST', $descriptorData);
    }

    public function deleteDescriptor($descriptorData, $location = 'L004')
    {
        return $this->makeRequest("/rapidstor/api/descriptors/delete?location=$location", 'POST', $descriptorData);
    }

    public function batchUpdate($operation, $descriptors, $location = 'L004')
    {
        $data = [
            'operation' => $operation,
            'descriptors' => $descriptors,
            'location' => $location
        ];
        return $this->makeRequest("/rapidstor/api/descriptors/batch", 'POST', $data);
    }

    public function getStatus()
    {
        return $this->makeRequest("/rapidstor/status");
    }

    public function login($forceRefresh = false)
    {
        $data = ['force_refresh' => $forceRefresh];
        return $this->makeRequest("/rapidstor/login", 'POST', $data);
    }

    public function getDeals($location = 'L004')
    {
        $endpoint = "/rapidstor/api/deals?location=" . urlencode($location);
        return $this->makeRequest($endpoint);
    }

    public function getInsurance($location = 'L004')
    {
        $endpoint = "/rapidstor/api/insurance?location=" . urlencode($location);
        return $this->makeRequest($endpoint);
    }

    public function getUnitTypes($location = 'L004')
    {
        $endpoint = "/rapidstor/api/unittypes?location=" . urlencode($location);
        return $this->makeRequest($endpoint);
    }
}

// ============================================================================
// INDEX.PHP - Main Application
// ============================================================================

session_start();

// Initialize variables
$jwtToken = $_SESSION['jwt_token'] ?? '';
$message = '';
$messageType = '';
$descriptors = [];
$deals = [];
$insurance = [];
$unitTypes = [];
$selectedLocation = $_GET['location'] ?? $_SESSION['location'] ?? 'L004';
$searchTerm = $_GET['search'] ?? '';
$sortBy = $_GET['sort'] ?? 'ordinalPosition';
$sortOrder = $_GET['order'] ?? 'asc';
$viewMode = $_GET['view'] ?? 'table';

// Handle JWT token input
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['jwt_token'])) {
    $jwtToken = trim($_POST['jwt_token']);
    $_SESSION['jwt_token'] = $jwtToken;
}

$api = new RapidStorAPI($jwtToken);
$_SESSION['location'] = $selectedLocation;

// ============================================================================
// AJAX HANDLERS
// ============================================================================

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $action = $_POST['action'] ?? '';

    // Handle AJAX requests
    if (in_array($action, ['quick_toggle', 'reorder_descriptors', 'group_descriptors'])) {
        header('Content-Type: application/json');
        
        try {
            switch ($action) {
                case 'quick_toggle':
                    $descriptorId = $_POST['descriptor_id'];
                    $field = $_POST['field'];
                    $value = $_POST['value'] === 'true';

                    // Get current descriptor data
                    $descriptorsResult = $api->getDescriptors($selectedLocation);
                    if ($descriptorsResult['status'] === 200) {
                        $allDescriptors = $descriptorsResult['data']['data'] ?? [];
                        $descriptor = null;
                        
                        foreach ($allDescriptors as $desc) {
                            if ($desc['_id'] === $descriptorId) {
                                $descriptor = $desc;
                                break;
                            }
                        }

                        if ($descriptor) {
                            $descriptor[$field] = $value;
                            $result = $api->saveDescriptor($descriptor, $selectedLocation);
                            
                            if ($result['status'] === 200) {
                                echo json_encode(['success' => true, 'message' => 'Updated successfully']);
                            } else {
                                echo json_encode(['success' => false, 'error' => 'Failed to update']);
                            }
                        } else {
                            echo json_encode(['success' => false, 'error' => 'Descriptor not found']);
                        }
                    } else {
                        echo json_encode(['success' => false, 'error' => 'Failed to load descriptors']);
                    }
                    exit;

                case 'reorder_descriptors':
                    $orderedIds = json_decode($_POST['ordered_ids'], true);
                    
                    // Get current descriptors
                    $descriptorsResult = $api->getDescriptors($selectedLocation);
                    if ($descriptorsResult['status'] === 200) {
                        $allDescriptors = $descriptorsResult['data']['data'] ?? [];
                        $descriptorMap = [];
                        
                        foreach ($allDescriptors as $desc) {
                            $descriptorMap[$desc['_id']] = $desc;
                        }

                        // Update ordinal positions
                        $updatedDescriptors = [];
                        foreach ($orderedIds as $index => $id) {
                            if (isset($descriptorMap[$id])) {
                                $descriptorMap[$id]['ordinalPosition'] = $index + 1;
                                $updatedDescriptors[] = $descriptorMap[$id];
                            }
                        }

                        // Batch update
                        if (!empty($updatedDescriptors)) {
                            $result = $api->batchUpdate('save', $updatedDescriptors, $selectedLocation);
                            if ($result['status'] === 200) {
                                echo json_encode(['success' => true, 'message' => 'Order updated successfully']);
                            } else {
                                echo json_encode(['success' => false, 'error' => 'Failed to update order']);
                            }
                        }
                    } else {
                        echo json_encode(['success' => false, 'error' => 'Failed to load descriptors']);
                    }
                    exit;

                case 'group_descriptors':
                    $descriptorIds = json_decode($_POST['descriptor_ids'], true);
                    $groupName = $_POST['group_name'];
                    
                    echo json_encode(['success' => true, 'message' => 'Grouping feature coming soon']);
                    exit;
            }
        } catch (Exception $e) {
            echo json_encode(['success' => false, 'error' => $e->getMessage()]);
            exit;
        }
    }

    // Handle regular form submissions
    try {
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

            case 'save_descriptor':
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
                    $descriptorData['_id'] = $_POST['_id'];
                    
                    $existingResult = $api->getDescriptors($selectedLocation);
                    if ($existingResult['status'] === 200) {
                        $existingDescriptors = $existingResult['data']['data'] ?? [];
                        $existingDescriptor = null;

                        foreach ($existingDescriptors as $desc) {
                            if ($desc['_id'] === $_POST['_id']) {
                                $existingDescriptor = $desc;
                                break;
                            }
                        }

                        if ($existingDescriptor) {
                            $descriptorData = array_merge($existingDescriptor, $descriptorData);
                        }
                    }
                } else {
                    $descriptorData = array_merge($descriptorData, [
                        'descriptions' => [''],
                        'spacerEnabled' => false,
                        'criteria' => [
                            'include' => [
                                'sizes' => [],
                                'keywords' => [],
                                'floors' => [],
                                'features' => ['climate' => null, 'inside' => null, 'alarm' => null, 'power' => null],
                                'prices' => []
                            ],
                            'exclude' => [
                                'sizes' => [],
                                'keywords' => [],
                                'floors' => [],
                                'features' => ['climate' => null, 'inside' => null, 'alarm' => null, 'power' => null],
                                'prices' => []
                            ]
                        ],
                        'deals' => [],
                        'tags' => [],
                        'upgradesTo' => [],
                        'slides' => [],
                        'picture' => 'https://ik.imagekit.io/bytcx9plm/150x150.png',
                        'highlight' => ['use' => false, 'colour' => '#ffffff', 'flag' => false],
                        'defaultInsuranceCoverage' => '6723aaef41549342379e4dfd'
                    ]);
                }

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

            case 'delete_descriptor':
                $descriptorData = json_decode($_POST['descriptor_data'], true);
                $result = $api->deleteDescriptor($descriptorData, $selectedLocation);
                if ($result['status'] === 200) {
                    $message = 'Descriptor deleted successfully';
                    $messageType = 'success';
                } else {
                    $message = 'Delete failed: ' . ($result['data']['error'] ?? 'Unknown error');
                    $messageType = 'error';
                }
                break;

            case 'batch_update':
                $operation = $_POST['operation'];
                $selectedIds = json_decode($_POST['selected_ids'], true);

                $allDescriptors = [];
                $descriptorsResult = $api->getDescriptors($selectedLocation);
                if ($descriptorsResult['status'] === 200) {
                    $allDescriptors = $descriptorsResult['data']['data'] ?? [];
                }

                $selectedDescriptors = array_filter($allDescriptors, function($desc) use ($selectedIds) {
                    return in_array($desc['_id'], $selectedIds);
                });

                switch ($operation) {
                    case 'enable':
                        $selectedDescriptors = array_map(function($desc) {
                            $desc['enabled'] = true;
                            return $desc;
                        }, $selectedDescriptors);
                        $operation = 'save';
                        break;
                    case 'disable':
                        $selectedDescriptors = array_map(function($desc) {
                            $desc['enabled'] = false;
                            return $desc;
                        }, $selectedDescriptors);
                        $operation = 'save';
                        break;
                    case 'show':
                        $selectedDescriptors = array_map(function($desc) {
                            $desc['hidden'] = false;
                            return $desc;
                        }, $selectedDescriptors);
                        $operation = 'save';
                        break;
                    case 'hide':
                        $selectedDescriptors = array_map(function($desc) {
                            $desc['hidden'] = true;
                            return $desc;
                        }, $selectedDescriptors);
                        $operation = 'save';
                        break;
                }

                $result = $api->batchUpdate($operation, array_values($selectedDescriptors), $selectedLocation);
                if ($result['status'] === 200) {
                    $message = $result['data']['summary'] ?? 'Batch operation completed';
                    $messageType = $result['data']['success'] ? 'success' : 'warning';
                } else {
                    $message = 'Batch operation failed: ' . ($result['data']['error'] ?? 'Unknown error');
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
// LOAD DATA
// ============================================================================

if (!$api->hasValidToken()) {
    $message = 'Please provide a valid JWT token to access the API.';
    $messageType = 'error';
} else {
    try {
        // Load descriptors
        $result = $api->getDescriptors($selectedLocation);
        if ($result['status'] === 200) {
            $descriptors = $result['data']['data'] ?? [];
            if (empty($descriptors) && !empty($result['data'])) {
                $descriptors = is_array($result['data']) ? $result['data'] : [];
            }
        } else {
            $errorMsg = 'Unknown error';
            if (isset($result['data']['error'])) {
                $errorMsg = $result['data']['error'];
            } elseif (isset($result['data']['message'])) {
                $errorMsg = $result['data']['message'];
            } elseif (isset($result['data']['msg'])) {
                $errorMsg = $result['data']['msg'];
            } elseif (!empty($result['raw'])) {
                $errorMsg = "HTTP {$result['status']}: " . substr($result['raw'], 0, 200);
            }

            $message = "Failed to load descriptors: $errorMsg";
            $messageType = 'error';
        }

        // Load deals
        try {
            $dealsResult = $api->getDeals($selectedLocation);
            if ($dealsResult['status'] === 200) {
                $deals = $dealsResult['data'] ?? [];
                if (isset($deals['data'])) {
                    $deals = $deals['data'];
                }
            }
        } catch (Exception $e) {
            // Silent fail for deals
        }

        // Load insurance
        try {
            $insuranceResult = $api->getInsurance($selectedLocation);
            if ($insuranceResult['status'] === 200) {
                $insurance = $insuranceResult['data'] ?? [];
                if (isset($insurance['data'])) {
                    $insurance = $insurance['data'];
                }
            }
        } catch (Exception $e) {
            // Silent fail for insurance
        }

        // Load unit types with detailed logging
        try {
            $unitTypesResult = $api->getUnitTypes($selectedLocation);
            error_log("Unit types API response status: " . $unitTypesResult['status']);
            error_log("Unit types raw response: " . substr($unitTypesResult['raw'] ?? '', 0, 500));
            
            if ($unitTypesResult['status'] === 200) {
                $unitTypes = $unitTypesResult['data'] ?? [];
                if (isset($unitTypes['data'])) {
                    $unitTypes = $unitTypes['data'];
                }
                error_log("Unit types loaded: " . count($unitTypes) . " items");
                
                // Log a sample unit type for debugging
                if (!empty($unitTypes)) {
                    error_log("Sample unit type: " . json_encode(reset($unitTypes)));
                }
            } else {
                error_log("Failed to load unit types: " . ($unitTypesResult['data']['error'] ?? 'Unknown error'));
            }
        } catch (Exception $e) {
            error_log("Exception loading unit types: " . $e->getMessage());
            // Don't fail silently - set an empty array
            $unitTypes = [];
        }

    } catch (Exception $e) {
        $message = 'Error loading data: ' . $e->getMessage();
        $messageType = 'error';
    }
}

// Create lookup arrays
$dealsLookup = [];
foreach ($deals as $deal) {
    if (isset($deal['_id'])) {
        $dealsLookup[$deal['_id']] = $deal;
    }
}

$insuranceLookup = [];
foreach ($insurance as $coverage) {
    if (isset($coverage['_id'])) {
        $insuranceLookup[$coverage['_id']] = $coverage;
    }
}

$unitTypesLookup = [];
foreach ($unitTypes as $unitType) {
    if (isset($unitType['_id'])) {
        $unitTypesLookup[$unitType['_id']] = $unitType;
    }
}

// Calculate inventory for each descriptor
function calculateInventory($descriptor, $unitTypesLookup) {
    $inventory = [
        'total' => 0,
        'occupied' => 0,
        'reserved' => 0,
        'vacant' => 0,
        'availability' => 0
    ];

    // Debug: Log what we're working with
    error_log("Calculating inventory for descriptor: " . ($descriptor['name'] ?? 'Unknown'));
    error_log("Descriptor criteria: " . json_encode($descriptor['criteria'] ?? []));
    error_log("Available unit types: " . count($unitTypesLookup));

    // Check multiple possible ways descriptors might reference unit types
    $unitTypeIds = [];
    
    // Method 1: Check criteria.include.sizes
    if (isset($descriptor['criteria']['include']['sizes']) && is_array($descriptor['criteria']['include']['sizes'])) {
        $unitTypeIds = array_merge($unitTypeIds, $descriptor['criteria']['include']['sizes']);
        error_log("Found unit IDs in criteria.include.sizes: " . json_encode($descriptor['criteria']['include']['sizes']));
    }
    
    // Method 2: Check if there's a direct unitTypes field
    if (isset($descriptor['unitTypes']) && is_array($descriptor['unitTypes'])) {
        $unitTypeIds = array_merge($unitTypeIds, $descriptor['unitTypes']);
        error_log("Found unit IDs in unitTypes: " . json_encode($descriptor['unitTypes']));
    }
    
    // Method 3: Check upgradesTo for related unit types
    if (isset($descriptor['upgradesTo']) && is_array($descriptor['upgradesTo'])) {
        foreach ($descriptor['upgradesTo'] as $upgrade) {
            if (isset($upgrade['_id'])) {
                $unitTypeIds[] = $upgrade['_id'];
            }
        }
        error_log("Found unit IDs in upgradesTo: " . json_encode(array_column($descriptor['upgradesTo'], '_id')));
    }
    
    // Method 4: Try to match by name patterns (fallback)
    if (empty($unitTypeIds)) {
        $descriptorName = strtolower($descriptor['name'] ?? '');
        foreach ($unitTypesLookup as $unitTypeId => $unitType) {
            $unitTypeName = strtolower($unitType['name'] ?? '');
            
            // Extract size patterns from both names
            preg_match('/(\d+(?:\.\d+)?(?:\s*x\s*\d+(?:\.\d+)?)?)\s*(?:sq\s*ft|sqft|\'|feet|ft)/i', $descriptorName, $descMatches);
            preg_match('/(\d+(?:\.\d+)?(?:\s*x\s*\d+(?:\.\d+)?)?)\s*(?:sq\s*ft|sqft|\'|feet|ft)/i', $unitTypeName, $unitMatches);
            
            if (!empty($descMatches[1]) && !empty($unitMatches[1])) {
                if (trim($descMatches[1]) === trim($unitMatches[1])) {
                    $unitTypeIds[] = $unitTypeId;
                    error_log("Matched by size pattern: {$descriptorName} -> {$unitTypeName}");
                }
            }
        }
    }
    
    // Remove duplicates
    $unitTypeIds = array_unique($unitTypeIds);
    error_log("Final unit type IDs for calculation: " . json_encode($unitTypeIds));

    // Calculate inventory from matched unit types
    foreach ($unitTypeIds as $unitTypeId) {
        if (isset($unitTypesLookup[$unitTypeId])) {
            $unitType = $unitTypesLookup[$unitTypeId];
            error_log("Processing unit type: " . json_encode($unitType));
            
            $inventory['total'] += intval($unitType['iTotalUnits'] ?? 0);
            $inventory['occupied'] += intval($unitType['iTotalOccupied'] ?? 0);
            $inventory['reserved'] += intval($unitType['iTotalReserved'] ?? 0);
            $inventory['vacant'] += intval($unitType['iTotalVacant'] ?? 0);
        } else {
            error_log("Unit type not found: " . $unitTypeId);
        }
    }

    $inventory['availability'] = $inventory['total'] > 0 ? 
        round(($inventory['vacant'] / $inventory['total']) * 100, 1) : 0;

    error_log("Final inventory for {$descriptor['name']}: " . json_encode($inventory));
    return $inventory;
}

// Add inventory data to descriptors with better debugging
foreach ($descriptors as &$descriptor) {
    $descriptor['inventory'] = calculateInventory($descriptor, $unitTypesLookup);
}

// Debug: Log some stats
error_log("Processing complete:");
error_log("- Total descriptors: " . count($descriptors));
error_log("- Total unit types in lookup: " . count($unitTypesLookup));
error_log("- Sample unit type IDs: " . json_encode(array_slice(array_keys($unitTypesLookup), 0, 5)));

// Log a few descriptor inventory results for debugging
foreach (array_slice($descriptors, 0, 3) as $desc) {
    error_log("Descriptor '{$desc['name']}' inventory: " . json_encode($desc['inventory']));
}

// Filter descriptors based on search
if ($searchTerm) {
    $descriptors = array_filter($descriptors, function($desc) use ($searchTerm) {
        $searchLower = strtolower($searchTerm);
        return strpos(strtolower($desc['name'] ?? ''), $searchLower) !== false ||
               strpos(strtolower($desc['description'] ?? ''), $searchLower) !== false ||
               strpos(strtolower($desc['specialText'] ?? ''), $searchLower) !== false;
    });
}

// Sort descriptors
usort($descriptors, function($a, $b) use ($sortBy, $sortOrder) {
    $valueA = $a[$sortBy] ?? '';
    $valueB = $b[$sortBy] ?? '';
    
    if (is_numeric($valueA) && is_numeric($valueB)) {
        $result = $valueA <=> $valueB;
    } else {
        $result = strcasecmp($valueA, $valueB);
    }
    
    return $sortOrder === 'desc' ? -$result : $result;
});

// Group descriptors by size if in grouped view
$groupedDescriptors = [];
if ($viewMode === 'grouped') {
    foreach ($descriptors as $descriptor) {
        $sizeName = 'Ungrouped';
        if (preg_match('/(\d+(?:\.\d+)?(?:-\d+(?:\.\d+)?)?)\s*(?:sq\s*ft|sqft|square\s*feet)/i', $descriptor['name'], $matches)) {
            $sizeName = $matches[1] . ' sq ft';
        }
        
        if (!isset($groupedDescriptors[$sizeName])) {
            $groupedDescriptors[$sizeName] = [];
        }
        $groupedDescriptors[$sizeName][] = $descriptor;
    }
}

// Get editing descriptor
$editingDescriptor = null;
if (isset($_GET['edit'])) {
    $editingId = $_GET['edit'];
    $editingDescriptor = array_filter($descriptors, function($desc) use ($editingId) {
        return $desc['_id'] === $editingId;
    });
    $editingDescriptor = reset($editingDescriptor);
}

?><!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Enhanced RapidStor Descriptor Manager</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/sortablejs@1.15.0/Sortable.min.js"></script>
    <style>
        .sortable-ghost {
            opacity: 0.4;
            background: #f3f4f6;
        }
        .sortable-chosen {
            transform: rotate(5deg);
        }
        .sortable-drag {
            opacity: 0.8;
            transform: rotate(5deg);
        }
        .group-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }
        .status-toggle {
            transition: all 0.2s ease;
        }
        .status-toggle:hover {
            transform: scale(1.1);
        }
        .inventory-bar {
            height: 4px;
            border-radius: 2px;
            overflow: hidden;
        }
        .inventory-segment {
            height: 100%;
            display: inline-block;
        }
    </style>
</head>
<body class="bg-gray-50 min-h-screen">

<div class="max-w-7xl mx-auto p-6">
    <div class="bg-white rounded-lg shadow-lg">

        <!-- Header -->
        <div class="border-b border-gray-200 p-6">

            <!-- JWT Token Input (show if not authenticated) -->
            <?php if (!$api->hasValidToken()): ?>
            <div class="mb-6 p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
                <h3 class="text-lg font-semibold text-yellow-800 mb-2">Authentication Required</h3>
                <p class="text-yellow-700 mb-3">Please enter your JWT token to access the RapidStor API:</p>
                <form method="post" class="flex gap-2">
                    <input type="text" name="jwt_token" placeholder="Enter your JWT token..."
                           value="<?= htmlspecialchars($jwtToken) ?>"
                           class="flex-1 border border-yellow-300 rounded-md px-3 py-2 text-sm">
                    <button type="submit" class="bg-yellow-600 hover:bg-yellow-700 text-white px-4 py-2 rounded">
                        <i class="fas fa-key mr-2"></i>Authenticate
                    </button>
                </form>
            </div>
            <?php endif; ?>

            <div class="flex justify-between items-center mb-4">
                <h1 class="text-3xl font-bold text-gray-900">Enhanced RapidStor Manager</h1>
                <div class="flex gap-2">
                    <?php if ($api->hasValidToken()): ?>
                    <form method="post" class="inline">
                        <input type="hidden" name="action" value="login">
                        <button type="submit" class="bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded-lg">
                            <i class="fas fa-sign-in-alt mr-2"></i>Login to RapidStor
                        </button>
                    </form>
                    <a href="?create=1" class="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg inline-flex items-center">
                        <i class="fas fa-plus mr-2"></i>Create New
                    </a>
                    <form method="post" class="inline">
                        <input type="hidden" name="jwt_token" value="">
                        <button type="submit" class="bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded-lg">
                            <i class="fas fa-sign-out-alt mr-2"></i>Logout
                        </button>
                    </form>
                    <?php endif; ?>
                </div>
            </div>

            <!-- Enhanced Controls -->
            <?php if ($api->hasValidToken()): ?>
            <div class="space-y-4">
                <!-- First row: Location, Search, View Mode -->
                <form method="get" class="flex flex-wrap gap-4 items-center">
                    <!-- Location Selector -->
                    <div class="flex items-center gap-2">
                        <label class="text-sm font-medium text-gray-700">Location:</label>
                        <select name="location" onchange="this.form.submit()" class="border border-gray-300 rounded-md px-3 py-2 text-sm">
                            <?php foreach (Config::LOCATIONS as $code => $name): ?>
                            <option value="<?= htmlspecialchars($code) ?>" <?= $selectedLocation === $code ? 'selected' : '' ?>>
                                <?= htmlspecialchars("$code - $name") ?>
                            </option>
                            <?php endforeach; ?>
                        </select>
                    </div>

                    <!-- Search -->
                    <div class="flex items-center gap-2 flex-1 min-w-64">
                        <i class="fas fa-search text-gray-400"></i>
                        <input type="text" name="search" placeholder="Search descriptors..."
                               value="<?= htmlspecialchars($searchTerm) ?>"
                               class="border border-gray-300 rounded-md px-3 py-2 text-sm flex-1">
                        <button type="submit" class="bg-gray-600 hover:bg-gray-700 text-white px-3 py-2 rounded text-sm">
                            Search
                        </button>
                    </div>

                    <!-- View Mode Toggle -->
                    <div class="flex items-center gap-2">
                        <label class="text-sm font-medium text-gray-700">View:</label>
                        <select name="view" onchange="this.form.submit()" class="border border-gray-300 rounded-md px-3 py-2 text-sm">
                            <option value="table" <?= $viewMode === 'table' ? 'selected' : '' ?>>Table View</option>
                            <option value="grouped" <?= $viewMode === 'grouped' ? 'selected' : '' ?>>Grouped by Size</option>
                        </select>
                    </div>

                    <!-- Hidden fields to preserve state -->
                    <input type="hidden" name="sort" value="<?= htmlspecialchars($sortBy) ?>">
                    <input type="hidden" name="order" value="<?= htmlspecialchars($sortOrder) ?>">
                </form>

                <!-- Second row: Sort controls and Quick Actions -->
                <div class="flex justify-between items-center">
                    <div class="flex items-center gap-4">
                        <span class="text-sm text-gray-600">Sort by:</span>
                        <a href="?<?= http_build_query(array_merge($_GET, ['sort' => 'ordinalPosition', 'order' => $sortBy === 'ordinalPosition' && $sortOrder === 'asc' ? 'desc' : 'asc'])) ?>" 
                           class="text-sm text-blue-600 hover:text-blue-800">
                            Position <?= $sortBy === 'ordinalPosition' ? ($sortOrder === 'asc' ? '↑' : '↓') : '' ?>
                        </a>
                        <a href="?<?= http_build_query(array_merge($_GET, ['sort' => 'name', 'order' => $sortBy === 'name' && $sortOrder === 'asc' ? 'desc' : 'asc'])) ?>" 
                           class="text-sm text-blue-600 hover:text-blue-800">
                            Name <?= $sortBy === 'name' ? ($sortOrder === 'asc' ? '↑' : '↓') : '' ?>
                        </a>
                        <a href="?<?= http_build_query(array_merge($_GET, ['sort' => 'enabled', 'order' => $sortBy === 'enabled' && $sortOrder === 'asc' ? 'desc' : 'asc'])) ?>" 
                           class="text-sm text-blue-600 hover:text-blue-800">
                            Status <?= $sortBy === 'enabled' ? ($sortOrder === 'asc' ? '↑' : '↓') : '' ?>
                        </a>
                    </div>

                    <div class="flex items-center gap-2">
                        <button onclick="enableDragDrop()" id="dragToggle" class="bg-purple-600 hover:bg-purple-700 text-white px-3 py-1 rounded text-sm">
                            <i class="fas fa-arrows-alt mr-1"></i>Enable Drag & Drop
                        </button>
                        <button onclick="bulkToggle('enabled', true)" class="bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm">
                            <i class="fas fa-toggle-on mr-1"></i>Enable All
                        </button>
                        <button onclick="bulkToggle('enabled', false)" class="bg-gray-600 hover:bg-gray-700 text-white px-3 py-1 rounded text-sm">
                            <i class="fas fa-toggle-off mr-1"></i>Disable All
                        </button>
                    </div>
                </div>
            </div>
            <?php endif; ?>

            <!-- Message -->
            <?php if ($message): ?>
            <div class="mt-4 p-3 rounded-md <?= $messageType === 'success' ? 'bg-green-50 text-green-800 border border-green-200' :
                ($messageType === 'error' ? 'bg-red-50 text-red-800 border border-red-200' : 'bg-yellow-50 text-yellow-800 border border-yellow-200') ?>">
                <?= htmlspecialchars($message) ?>
            </div>
            <?php endif; ?>
        </div>

        <!-- Create/Edit Form -->
        <?php if (isset($_GET['create']) || $editingDescriptor): ?>
        <div class="border-b border-gray-200 p-6 bg-gray-50">
            <h3 class="text-lg font-semibold mb-4">
                <?= isset($_GET['create']) ? 'Create New Descriptor' : 'Edit Descriptor' ?>
            </h3>
            <form method="post">
                <input type="hidden" name="action" value="save_descriptor">
                <?php if ($editingDescriptor): ?>
                <input type="hidden" name="_id" value="<?= htmlspecialchars($editingDescriptor['_id']) ?>">
                <?php endif; ?>

                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                        <label class="block text-sm font-medium text-gray-700 mb-1">Name</label>
                        <input type="text" name="name" required
                               value="<?= htmlspecialchars($editingDescriptor['name'] ?? '') ?>"
                               class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm">
                    </div>
                    <div>
                        <label class="block text-sm font-medium text-gray-700 mb-1">Description</label>
                        <input type="text" name="description"
                               value="<?= htmlspecialchars($editingDescriptor['description'] ?? '') ?>"
                               class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm">
                    </div>
                    <div>
                        <label class="block text-sm font-medium text-gray-700 mb-1">Special Text</label>
                        <input type="text" name="specialText"
                               value="<?= htmlspecialchars($editingDescriptor['specialText'] ?? '') ?>"
                               class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm">
                    </div>
                    <div>
                        <label class="block text-sm font-medium text-gray-700 mb-1">Ordinal Position</label>
                        <input type="number" name="ordinalPosition"
                               value="<?= htmlspecialchars($editingDescriptor['ordinalPosition'] ?? (count($descriptors) + 1)) ?>"
                               class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm">
                    </div>
                    <div class="flex items-center gap-4 col-span-2">
                        <label class="flex items-center gap-2">
                            <input type="checkbox" name="enabled" <?= ($editingDescriptor['enabled'] ?? true) ? 'checked' : '' ?>>
                            <span class="text-sm">Enabled</span>
                        </label>
                        <label class="flex items-center gap-2">
                            <input type="checkbox" name="visible" <?= !($editingDescriptor['hidden'] ?? false) ? 'checked' : '' ?>>
                            <span class="text-sm">Visible</span>
                        </label>
                        <label class="flex items-center gap-2">
                            <input type="checkbox" name="useForCarousel" <?= ($editingDescriptor['useForCarousel'] ?? true) ? 'checked' : '' ?>>
                            <span class="text-sm">Use for Carousel</span>
                        </label>
                    </div>
                </div>
                <div class="mt-4 flex gap-2">
                    <button type="submit" class="bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded flex items-center gap-2">
                        <i class="fas fa-save"></i>Save
                    </button>
                    <a href="?" class="bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded flex items-center gap-2">
                        <i class="fas fa-times"></i>Cancel
                    </a>
                </div>
            </form>
        </div>
        <?php endif; ?>

        <!-- Batch Actions -->
        <div id="batchActions" class="border-b border-gray-200 p-4 bg-gray-50" style="display: none;">
            <div class="flex items-center gap-2">
                <span id="selectedCount" class="text-sm text-gray-600">0 selected</span>

                <button onclick="batchAction('enable')" class="bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm">
                    Enable
                </button>
                <button onclick="batchAction('disable')" class="bg-yellow-600 hover:bg-yellow-700 text-white px-3 py-1 rounded text-sm">
                    Disable
                </button>
                <button onclick="batchAction('show')" class="bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded text-sm">
                    Show
                </button>
                <button onclick="batchAction('hide')" class="bg-gray-600 hover:bg-gray-700 text-white px-3 py-1 rounded text-sm">
                    Hide
                </button>
                <button onclick="batchAction('delete')" class="bg-red-600 hover:bg-red-700 text-white px-3 py-1 rounded text-sm">
                    Delete
                </button>
                <button onclick="groupSelected()" class="bg-indigo-600 hover:bg-indigo-700 text-white px-3 py-1 rounded text-sm">
                    <i class="fas fa-layer-group mr-1"></i>Group
                </button>
            </div>
        </div>

        <!-- Main Content Area -->
        <div class="overflow-x-auto">
            <?php if ($viewMode === 'grouped'): ?>
                <!-- Grouped View -->
                <div class="p-6 space-y-6">
                    <?php foreach ($groupedDescriptors as $groupName => $groupDescriptors): ?>
                    <div class="border border-gray-200 rounded-lg overflow-hidden">
                        <div class="group-header px-4 py-3 text-white">
                            <div class="flex items-center justify-between">
                                <h3 class="text-lg font-semibold flex items-center">
                                    <i class="fas fa-layer-group mr-2"></i>
                                    <?= htmlspecialchars($groupName) ?>
                                    <span class="ml-2 bg-white bg-opacity-20 px-2 py-1 rounded text-sm">
                                        <?= count($groupDescriptors) ?> descriptors
                                    </span>
                                </h3>
                                <div class="flex items-center gap-2">
                                    <?php
                                    $groupInventory = ['total' => 0, 'vacant' => 0, 'occupied' => 0];
                                    foreach ($groupDescriptors as $desc) {
                                        $groupInventory['total'] += $desc['inventory']['total'];
                                        $groupInventory['vacant'] += $desc['inventory']['vacant'];
                                        $groupInventory['occupied'] += $desc['inventory']['occupied'];
                                    }
                                    $groupAvailability = $groupInventory['total'] > 0 ? 
                                        round(($groupInventory['vacant'] / $groupInventory['total']) * 100, 1) : 0;
                                    ?>
                                    <span class="text-sm bg-white bg-opacity-20 px-2 py-1 rounded">
                                        <?= $groupAvailability ?>% available
                                    </span>
                                    <button onclick="toggleGroup('<?= htmlspecialchars($groupName) ?>')" class="text-white hover:bg-white hover:bg-opacity-20 p-1 rounded">
                                        <i class="fas fa-chevron-down group-toggle"></i>
                                    </button>
                                </div>
                            </div>
                        </div>
                        <div class="group-content" id="group-<?= htmlspecialchars($groupName) ?>">
                            <table class="w-full">
                                <tbody class="divide-y divide-gray-200" data-sortable="true">
                                    <?php foreach ($groupDescriptors as $descriptor): ?>
                                    <tr class="hover:bg-gray-50 sortable-item" data-id="<?= htmlspecialchars($descriptor['_id']) ?>">
                                        <td class="px-4 py-3">
                                            <input type="checkbox" class="descriptor-checkbox"
                                                   value="<?= htmlspecialchars($descriptor['_id']) ?>"
                                                   onchange="updateSelection()">
                                        </td>
                                        <td class="px-4 py-3">
                                            <div class="flex items-center gap-2">
                                                <?php if (!empty($descriptor['picture'])): ?>
                                                <img src="<?= htmlspecialchars($descriptor['picture']) ?>"
                                                     alt="<?= htmlspecialchars($descriptor['name']) ?>"
                                                     class="w-8 h-8 object-cover rounded">
                                                <?php endif; ?>
                                                <div>
                                                    <div class="text-sm font-medium text-gray-900">
                                                        <?= htmlspecialchars($descriptor['name']) ?>
                                                    </div>
                                                    <?php if (!empty($descriptor['specialText'])): ?>
                                                    <div class="text-xs text-gray-500 truncate max-w-32">
                                                        <?= htmlspecialchars($descriptor['specialText']) ?>
                                                    </div>
                                                    <?php endif; ?>
                                                </div>
                                            </div>
                                        </td>
                                        <td class="px-4 py-3">
                                            <div class="flex gap-1">
                                                <!-- Quick toggles for grouped view -->
                                                <button onclick="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'enabled', <?= $descriptor['enabled'] ? 'false' : 'true' ?>)"
                                                        class="p-1 rounded <?= $descriptor['enabled'] ? 'text-green-600 hover:bg-green-100' : 'text-gray-400 hover:bg-gray-100' ?>"
                                                        title="<?= $descriptor['enabled'] ? 'Disable' : 'Enable' ?>">
                                                    <i class="fas fa-power-off"></i>
                                                </button>
                                                <button onclick="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'hidden', <?= !$descriptor['hidden'] ? 'true' : 'false' ?>)"
                                                        class="p-1 rounded <?= !$descriptor['hidden'] ? 'text-blue-600 hover:bg-blue-100' : 'text-gray-400 hover:bg-gray-100' ?>"
                                                        title="<?= !$descriptor['hidden'] ? 'Hide' : 'Show' ?>">
                                                    <i class="fas fa-eye<?= $descriptor['hidden'] ? '-slash' : '' ?>"></i>
                                                </button>
                                                <button onclick="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'useForCarousel', <?= $descriptor['useForCarousel'] ? 'false' : 'true' ?>)"
                                                        class="p-1 rounded <?= $descriptor['useForCarousel'] ? 'text-purple-600 hover:bg-purple-100' : 'text-gray-400 hover:bg-gray-100' ?>"
                                                        title="<?= $descriptor['useForCarousel'] ? 'Remove from Carousel' : 'Add to Carousel' ?>">
                                                    <i class="fas fa-images"></i>
                                                </button>
                                            </div>
                                        </td>
                                        <td class="px-4 py-3 text-sm text-gray-600">
                                            <?= $descriptor['inventory']['total'] ?> units
                                            (<?= $descriptor['inventory']['availability'] ?>% available)
                                        </td>
                                    </tr>
                                    <?php endforeach; ?>
                                </tbody>
                            </table>
                        </div>
                    </div>
                    <?php endforeach; ?>
                </div>
            <?php else: ?>
                <!-- Table View -->
                <table class="w-full">
                    <thead class="bg-gray-50 border-b border-gray-200">
                        <tr>
                            <th class="px-4 py-3 text-left">
                                <input type="checkbox" id="selectAll" onchange="toggleSelectAll()">
                            </th>
                            <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">
                                <i class="fas fa-grip-vertical text-gray-400 mr-2"></i>Order
                            </th>
                            <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Name</th>
                            <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Description</th>
                            <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Quick Controls</th>
                            <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Inventory</th>
                            <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Deals & Insurance</th>
                            <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Actions</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-gray-200" id="sortableTable" data-sortable="true">
                        <?php foreach ($descriptors as $descriptor): ?>
                        <tr class="hover:bg-gray-50 sortable-item" data-id="<?= htmlspecialchars($descriptor['_id']) ?>">
                            <td class="px-4 py-3">
                                <input type="checkbox" class="descriptor-checkbox"
                                       value="<?= htmlspecialchars($descriptor['_id']) ?>"
                                       onchange="updateSelection()">
                            </td>
                            <td class="px-4 py-3 text-sm text-gray-900">
                                <div class="flex items-center gap-2">
                                    <i class="fas fa-grip-vertical text-gray-400 drag-handle cursor-move"></i>
                                    <span class="bg-gray-100 px-2 py-1 rounded text-xs font-mono">
                                        <?= htmlspecialchars($descriptor['ordinalPosition'] ?? 0) ?>
                                    </span>
                                </div>
                            </td>
                            <td class="px-4 py-3">
                                <div class="flex items-center gap-2">
                                    <?php if (!empty($descriptor['picture'])): ?>
                                    <img src="<?= htmlspecialchars($descriptor['picture']) ?>"
                                         alt="<?= htmlspecialchars($descriptor['name']) ?>"
                                         class="w-10 h-10 object-cover rounded-lg border border-gray-200">
                                    <?php endif; ?>
                                    <div>
                                        <div class="text-sm font-medium text-gray-900">
                                            <?= htmlspecialchars($descriptor['name']) ?>
                                        </div>
                                        <?php if (!empty($descriptor['specialText'])): ?>
                                        <div class="text-xs text-gray-500 truncate max-w-32">
                                            <?= htmlspecialchars($descriptor['specialText']) ?>
                                        </div>
                                        <?php endif; ?>
                                    </div>
                                </div>
                            </td>
                            <td class="px-4 py-3">
                                <div class="text-sm text-gray-900 max-w-64">
                                    <?= htmlspecialchars($descriptor['description'] ?? '') ?>
                                </div>
                            </td>
                            <td class="px-4 py-3">
                                <div class="flex flex-col gap-1">
                                    <!-- Enabled Toggle -->
                                    <label class="flex items-center gap-2 cursor-pointer">
                                        <input type="checkbox" 
                                               class="status-toggle sr-only" 
                                               <?= $descriptor['enabled'] ? 'checked' : '' ?>
                                               onchange="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'enabled', this.checked)">
                                        <div class="relative inline-flex h-6 w-11 items-center rounded-full transition-colors
                                                    <?= $descriptor['enabled'] ? 'bg-green-600' : 'bg-gray-200' ?>">
                                            <span class="inline-block h-4 w-4 transform rounded-full bg-white transition-transform
                                                         <?= $descriptor['enabled'] ? 'translate-x-6' : 'translate-x-1' ?>"></span>
                                        </div>
                                        <span class="text-xs <?= $descriptor['enabled'] ? 'text-green-600' : 'text-gray-500' ?>">
                                            <?= $descriptor['enabled'] ? 'Enabled' : 'Disabled' ?>
                                        </span>
                                    </label>

                                    <!-- Visibility Toggle -->
                                    <label class="flex items-center gap-2 cursor-pointer">
                                        <input type="checkbox" 
                                               class="status-toggle sr-only" 
                                               <?= !$descriptor['hidden'] ? 'checked' : '' ?>
                                               onchange="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'hidden', !this.checked)">
                                        <div class="relative inline-flex h-6 w-11 items-center rounded-full transition-colors
                                                    <?= !$descriptor['hidden'] ? 'bg-blue-600' : 'bg-gray-200' ?>">
                                            <span class="inline-block h-4 w-4 transform rounded-full bg-white transition-transform
                                                         <?= !$descriptor['hidden'] ? 'translate-x-6' : 'translate-x-1' ?>"></span>
                                        </div>
                                        <span class="text-xs <?= !$descriptor['hidden'] ? 'text-blue-600' : 'text-gray-500' ?>">
                                            <?= !$descriptor['hidden'] ? 'Visible' : 'Hidden' ?>
                                        </span>
                                    </label>

                                    <!-- Carousel Toggle -->
                                    <label class="flex items-center gap-2 cursor-pointer">
                                        <input type="checkbox" 
                                               class="status-toggle sr-only" 
                                               <?= $descriptor['useForCarousel'] ? 'checked' : '' ?>
                                               onchange="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'useForCarousel', this.checked)">
                                        <div class="relative inline-flex h-6 w-11 items-center rounded-full transition-colors
                                                    <?= $descriptor['useForCarousel'] ? 'bg-purple-600' : 'bg-gray-200' ?>">
                                            <span class="inline-block h-4 w-4 transform rounded-full bg-white transition-transform
                                                         <?= $descriptor['useForCarousel'] ? 'translate-x-6' : 'translate-x-1' ?>"></span>
                                        </div>
                                        <span class="text-xs <?= $descriptor['useForCarousel'] ? 'text-purple-600' : 'text-gray-500' ?>">
                                            Carousel
                                        </span>
                                    </label>
                                </div>
                            </td>
                            <td class="px-4 py-3">
                                <div class="text-xs space-y-2">
                                    <!-- Inventory Summary -->
                                    <div class="flex items-center gap-2">
                                        <span class="text-gray-600">Units:</span>
                                        <span class="font-medium"><?= $descriptor['inventory']['total'] ?></span>
                                    </div>
                                    
                                    <!-- Availability Percentage -->
                                    <div class="flex items-center gap-2">
                                        <span class="text-gray-600">Available:</span>
                                        <span class="font-medium <?= $descriptor['inventory']['availability'] > 50 ? 'text-green-600' : 
                                            ($descriptor['inventory']['availability'] > 20 ? 'text-yellow-600' : 'text-red-600') ?>">
                                            <?= $descriptor['inventory']['availability'] ?>%
                                        </span>
                                    </div>

                                    <!-- Visual Inventory Bar -->
                                    <div class="inventory-bar bg-gray-200">
                                        <?php if ($descriptor['inventory']['total'] > 0): ?>
                                        <?php 
                                        $occupiedPercent = ($descriptor['inventory']['occupied'] / $descriptor['inventory']['total']) * 100;
                                        $reservedPercent = ($descriptor['inventory']['reserved'] / $descriptor['inventory']['total']) * 100;
                                        $vacantPercent = ($descriptor['inventory']['vacant'] / $descriptor['inventory']['total']) * 100;
                                        ?>
                                        <div class="inventory-segment bg-red-500" style="width: <?= $occupiedPercent ?>%" title="Occupied: <?= $descriptor['inventory']['occupied'] ?>"></div>
                                        <div class="inventory-segment bg-yellow-500" style="width: <?= $reservedPercent ?>%" title="Reserved: <?= $descriptor['inventory']['reserved'] ?>"></div>
                                        <div class="inventory-segment bg-green-500" style="width: <?= $vacantPercent ?>%" title="Vacant: <?= $descriptor['inventory']['vacant'] ?>"></div>
                                        <?php endif; ?>
                                    </div>

                                    <!-- Detailed Breakdown -->
                                    <div class="grid grid-cols-3 gap-1 text-xs">
                                        <div class="text-center">
                                            <div class="w-2 h-2 bg-red-500 rounded mx-auto mb-1"></div>
                                            <span><?= $descriptor['inventory']['occupied'] ?></span>
                                        </div>
                                        <div class="text-center">
                                            <div class="w-2 h-2 bg-yellow-500 rounded mx-auto mb-1"></div>
                                            <span><?= $descriptor['inventory']['reserved'] ?></span>
                                        </div>
                                        <div class="text-center">
                                            <div class="w-2 h-2 bg-green-500 rounded mx-auto mb-1"></div>
                                            <span><?= $descriptor['inventory']['vacant'] ?></span>
                                        </div>
                                    </div>
                                </div>
                            </td>
                            <td class="px-4 py-3">
                                <div class="text-xs text-gray-600 max-w-40">
                                    <!-- Deals -->
                                    <?php if (!empty($descriptor['deals']) && is_array($descriptor['deals'])): ?>
                                    <div class="mb-2">
                                        <div class="text-xs font-semibold text-blue-700 mb-1">Deals:</div>
                                        <?php foreach (array_slice($descriptor['deals'], 0, 1) as $dealId): ?>
                                            <?php if (isset($dealsLookup[$dealId])): ?>
                                            <div class="bg-blue-50 border border-blue-200 rounded px-2 py-1 mb-1">
                                                <div class="font-medium text-blue-800"><?= htmlspecialchars($dealsLookup[$dealId]['title']) ?></div>
                                                <?php if ($dealsLookup[$dealId]['enable']): ?>
                                                <div class="text-green-600 text-xs">✓ Active</div>
                                                <?php else: ?>
                                                <div class="text-gray-500 text-xs">○ Inactive</div>
                                                <?php endif; ?>
                                            </div>
                                            <?php endif; ?>
                                        <?php endforeach; ?>
                                        <?php if (count($descriptor['deals']) > 1): ?>
                                        <div class="text-xs text-blue-600">+ <?= count($descriptor['deals']) - 1 ?> more</div>
                                        <?php endif; ?>
                                    </div>
                                    <?php endif; ?>

                                    <!-- Insurance -->
                                    <?php if (!empty($descriptor['defaultInsuranceCoverage'])): ?>
                                    <div class="mb-1">
                                        <div class="text-xs font-semibold text-green-700 mb-1">Insurance:</div>
                                        <?php if (isset($insuranceLookup[$descriptor['defaultInsuranceCoverage']])): ?>
                                        <?php $coverage = $insuranceLookup[$descriptor['defaultInsuranceCoverage']]; ?>
                                        <div class="bg-green-50 border border-green-200 rounded px-2 py-1">
                                            <div class="font-medium text-green-800"><?= htmlspecialchars($coverage['sCoverageDesc']) ?></div>
                                            <div class="text-xs text-green-600">$<?= number_format($coverage['dcCoverage']) ?></div>
                                        </div>
                                        <?php else: ?>
                                        <div class="text-xs text-gray-500">ID: <?= htmlspecialchars(substr($descriptor['defaultInsuranceCoverage'], 0, 8)) ?>...</div>
                                        <?php endif; ?>
                                    </div>
                                    <?php endif; ?>
                                </div>
                            </td>
                            <td class="px-4 py-3">
                                <div class="flex items-center gap-2">
                                    <a href="?edit=<?= htmlspecialchars($descriptor['_id']) ?>"
                                       class="text-blue-600 hover:text-blue-800" title="Edit">
                                        <i class="fas fa-edit"></i>
                                    </a>

                                    <button onclick="deleteDescriptor('<?= htmlspecialchars($descriptor['_id']) ?>', '<?= htmlspecialchars($descriptor['name']) ?>')"
                                            class="text-red-600 hover:text-red-800" title="Delete">
                                        <i class="fas fa-trash"></i>
                                    </button>

                                    <button onclick="duplicateDescriptor('<?= htmlspecialchars($descriptor['_id']) ?>')"
                                            class="text-green-600 hover:text-green-800" title="Duplicate">
                                        <i class="fas fa-copy"></i>
                                    </button>
                                </div>
                            </td>
                        </tr>
                        <?php endforeach; ?>

                        <?php if (empty($descriptors)): ?>
                        <tr>
                            <td colspan="8" class="text-center py-12 text-gray-500">
                                <p class="text-lg">No descriptors found</p>
                                <?php if ($searchTerm): ?>
                                <p class="text-sm">Try adjusting your search criteria</p>
                                <?php endif; ?>
                            </td>
                        </tr>
                        <?php endif; ?>
                    </tbody>
                </table>
            <?php endif; ?>
        </div>

        <!-- Footer -->
        <div class="border-t border-gray-200 px-6 py-4 bg-gray-50">
            <div class="flex justify-between items-center text-sm text-gray-600">
                <div>
                    Showing <?= count($descriptors) ?> descriptors for location <?= htmlspecialchars($selectedLocation) ?>
                    <?php if ($searchTerm): ?>
                    (filtered by "<?= htmlspecialchars($searchTerm) ?>")
                    <?php endif; ?>
                    <div class="text-xs text-gray-500 mt-1">
                        Data loaded: <?= count($deals) ?> deals, <?= count($insurance) ?> insurance options, <?= count($unitTypes) ?> unit types
                    </div>
                </div>
                <div class="flex items-center gap-4">
                    <a href="?" class="text-blue-600 hover:text-blue-800">Refresh</a>
                    <span>Location: <?= htmlspecialchars($selectedLocation) ?></span>
                </div>
            </div>
        </div>

    </div>
</div>

<!-- Group Creation Modal -->
<div id="groupModal" class="fixed inset-0 bg-gray-600 bg-opacity-50 hidden z-50">
    <div class="flex items-center justify-center min-h-screen px-4">
        <div class="bg-white rounded-lg shadow-xl max-w-md w-full">
            <div class="flex justify-between items-center px-6 py-4 border-b border-gray-200">
                <h3 class="text-lg font-semibold text-gray-900">Create Group</h3>
                <button onclick="closeGroupModal()" class="text-gray-400 hover:text-gray-600">
                    <i class="fas fa-times"></i>
                </button>
            </div>
            <div class="px-6 py-4">
                <label class="block text-sm font-medium text-gray-700 mb-2">Group Name</label>
                <input type="text" id="groupName" placeholder="e.g., 8-10 sq ft" class="w-full border border-gray-300 rounded-md px-3 py-2">
                <p class="text-sm text-gray-500 mt-2">Selected descriptors will be grouped under this name.</p>
            </div>
            <div class="flex justify-end gap-2 px-6 py-4 border-t border-gray-200">
                <button onclick="closeGroupModal()" class="px-4 py-2 text-gray-600 hover:text-gray-800">Cancel</button>
                <button onclick="createGroup()" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded">Create Group</button>
            </div>
        </div>
    </div>
</div>

<script>
let selectedIds = new Set();
let dragDropEnabled = false;
let sortableInstance = null;

// Quick Toggle Functions
function quickToggle(descriptorId, field, value) {
    // Find the toggle element that was clicked
    const toggleElement = event.target.closest('label').querySelector('input');
    const statusLabel = event.target.closest('label').querySelector('span');
    const toggleSwitch = event.target.closest('label').querySelector('div');
    const toggleButton = toggleSwitch.querySelector('span');

    // Show loading state
    toggleSwitch.style.opacity = '0.6';
    
    const formData = new FormData();
    formData.append('action', 'quick_toggle');
    formData.append('descriptor_id', descriptorId);
    formData.append('field', field);
    formData.append('value', value.toString());

    fetch(window.location.href, {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            // Update the UI immediately after successful API call
            updateToggleUI(descriptorId, field, value);
            showToast('Updated successfully', 'success');
        } else {
            // Revert the toggle on failure
            toggleElement.checked = !value;
            showToast('Failed to update: ' + (data.error || 'Unknown error'), 'error');
        }
        // Remove loading state
        toggleSwitch.style.opacity = '1';
    })
    .catch(error => {
        // Revert the toggle on error
        toggleElement.checked = !value;
        toggleSwitch.style.opacity = '1';
        showToast('Network error: ' + error.message, 'error');
    });
}

function updateToggleUI(descriptorId, field, value) {
    // Find all toggles for this descriptor (in case there are multiple views)
    const descriptorRows = document.querySelectorAll(`tr[data-id="${descriptorId}"]`);
    
    descriptorRows.forEach(row => {
        const toggles = row.querySelectorAll('.status-toggle');
        let targetToggle = null;
        let targetIndex = 0;
        
        if (field === 'enabled') {
            targetIndex = 0;
        } else if (field === 'hidden') {
            targetIndex = 1;
            value = !value; // Invert for hidden field
        } else if (field === 'useForCarousel') {
            targetIndex = 2;
        }
        
        if (toggles[targetIndex]) {
            const toggle = toggles[targetIndex];
            const label = toggle.closest('label');
            const switchDiv = label.querySelector('div');
            const switchButton = switchDiv.querySelector('span');
            const statusText = label.querySelector('span');
            
            // Update checkbox state
            toggle.checked = value;
            
            // Update visual switch
            if (value) {
                switchDiv.className = switchDiv.className.replace('bg-gray-200', 'bg-green-600');
                if (field === 'hidden') switchDiv.className = switchDiv.className.replace('bg-green-600', 'bg-blue-600');
                if (field === 'useForCarousel') switchDiv.className = switchDiv.className.replace('bg-green-600', 'bg-purple-600');
                switchButton.className = switchButton.className.replace('translate-x-1', 'translate-x-6');
            } else {
                switchDiv.className = switchDiv.className.replace(/bg-(green|blue|purple)-600/, 'bg-gray-200');
                switchButton.className = switchButton.className.replace('translate-x-6', 'translate-x-1');
            }
            
            // Update status text and color
            if (field === 'enabled') {
                statusText.textContent = value ? 'Enabled' : 'Disabled';
                statusText.className = `text-xs ${value ? 'text-green-600' : 'text-gray-500'}`;
            } else if (field === 'hidden') {
                statusText.textContent = value ? 'Visible' : 'Hidden';
                statusText.className = `text-xs ${value ? 'text-blue-600' : 'text-gray-500'}`;
            } else if (field === 'useForCarousel') {
                statusText.textContent = 'Carousel';
                statusText.className = `text-xs ${value ? 'text-purple-600' : 'text-gray-500'}`;
            }
        }
    });
}

// Drag and Drop Functions
function enableDragDrop() {
    const button = document.getElementById('dragToggle');
    const sortableTable = document.getElementById('sortableTable');
    
    if (!dragDropEnabled) {
        // Enable drag and drop
        sortableInstance = Sortable.create(sortableTable, {
            animation: 150,
            ghostClass: 'sortable-ghost',
            chosenClass: 'sortable-chosen',
            dragClass: 'sortable-drag',
            handle: '.drag-handle',
            onEnd: function(evt) {
                updateOrder();
            }
        });
        
        dragDropEnabled = true;
        button.innerHTML = '<i class="fas fa-save mr-1"></i>Save Order';
        button.className = 'bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm';
        showToast('Drag and drop enabled. Drag rows to reorder.', 'info');
    } else {
        // Save order
        updateOrder();
    }
}

function updateOrder() {
    const rows = document.querySelectorAll('#sortableTable .sortable-item');
    const orderedIds = Array.from(rows).map(row => row.dataset.id);
    
    // Show loading state
    const button = document.getElementById('dragToggle');
    const originalText = button.innerHTML;
    button.innerHTML = '<i class="fas fa-spinner fa-spin mr-1"></i>Saving...';
    button.disabled = true;
    
    const formData = new FormData();
    formData.append('action', 'reorder_descriptors');
    formData.append('ordered_ids', JSON.stringify(orderedIds));

    // Increase timeout for this request
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 30000); // 30 second timeout

    fetch(window.location.href, {
        method: 'POST',
        body: formData,
        signal: controller.signal
    })
    .then(response => {
        clearTimeout(timeoutId);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        button.innerHTML = originalText;
        button.disabled = false;
        
        if (data.success) {
            showToast('Order updated successfully', 'success');
            // Reset drag and drop mode
            if (sortableInstance) {
                sortableInstance.destroy();
                sortableInstance = null;
            }
            dragDropEnabled = false;
            button.innerHTML = '<i class="fas fa-arrows-alt mr-1"></i>Enable Drag & Drop';
            button.className = 'bg-purple-600 hover:bg-purple-700 text-white px-3 py-1 rounded text-sm';
            
            // Update ordinal positions in the UI
            rows.forEach((row, index) => {
                const positionSpan = row.querySelector('.bg-gray-100 span');
                if (positionSpan) {
                    positionSpan.textContent = index + 1;
                }
            });
        } else {
            showToast('Failed to update order: ' + (data.error || 'Unknown error'), 'error');
        }
    })
    .catch(error => {
        clearTimeout(timeoutId);
        button.innerHTML = originalText;
        button.disabled = false;
        
        if (error.name === 'AbortError') {
            showToast('Request timed out. Please try again.', 'error');
        } else {
            showToast('Network error: ' + error.message, 'error');
        }
        console.error('Reorder error:', error);
    });
}

// Selection Functions
function toggleSelectAll() {
    const selectAll = document.getElementById('selectAll');
    const checkboxes = document.querySelectorAll('.descriptor-checkbox');

    checkboxes.forEach(checkbox => {
        checkbox.checked = selectAll.checked;
    });

    updateSelection();
}

function updateSelection() {
    const checkboxes = document.querySelectorAll('.descriptor-checkbox');
    const selectAll = document.getElementById('selectAll');

    selectedIds.clear();
    let checkedCount = 0;

    checkboxes.forEach(checkbox => {
        if (checkbox.checked) {
            selectedIds.add(checkbox.value);
            checkedCount++;
        }
    });

    // Update select all checkbox
    if (selectAll) {
        selectAll.checked = checkedCount === checkboxes.length && checkboxes.length > 0;
        selectAll.indeterminate = checkedCount > 0 && checkedCount < checkboxes.length;
    }

    // Show/hide batch actions
    const batchActions = document.getElementById('batchActions');
    const selectedCount = document.getElementById('selectedCount');

    if (selectedIds.size > 0) {
        batchActions.style.display = 'block';
        selectedCount.textContent = `${selectedIds.size} selected`;
    } else {
        batchActions.style.display = 'none';
    }
}

// Bulk Actions
function bulkToggle(field, value) {
    const checkboxes = document.querySelectorAll('.descriptor-checkbox');
    let count = 0;
    
    checkboxes.forEach(checkbox => {
        const row = checkbox.closest('tr');
        const descriptorId = checkbox.value;
        
        // Find the corresponding toggle in the row
        const toggles = row.querySelectorAll('.status-toggle');
        let targetToggle = null;
        
        if (field === 'enabled') {
            targetToggle = toggles[0];
        } else if (field === 'hidden') {
            targetToggle = toggles[1];
        } else if (field === 'useForCarousel') {
            targetToggle = toggles[2];
        }
        
        if (targetToggle && targetToggle.checked !== (field === 'hidden' ? !value : value)) {
            targetToggle.checked = (field === 'hidden' ? !value : value);
            quickToggle(descriptorId, field, value);
            count++;
        }
    });
    
    if (count > 0) {
        showToast(`Updated ${count} descriptors`, 'success');
    }
}

function batchAction(action) {
    if (selectedIds.size === 0) {
        showToast('Please select at least one descriptor', 'warning');
        return;
    }
    
    let confirmMessage = '';
    switch (action) {
        case 'delete':
            confirmMessage = `Are you sure you want to delete ${selectedIds.size} descriptors? This cannot be undone.`;
            break;
        case 'enable':
            confirmMessage = `Enable ${selectedIds.size} selected descriptors?`;
            break;
        case 'disable':
            confirmMessage = `Disable ${selectedIds.size} selected descriptors?`;
            break;
        case 'show':
            confirmMessage = `Make ${selectedIds.size} selected descriptors visible?`;
            break;
        case 'hide':
            confirmMessage = `Hide ${selectedIds.size} selected descriptors?`;
            break;
    }
    
    if (action === 'delete' && !confirm(confirmMessage)) {
        return;
    }
    
    // Perform the batch action
    const selectedArray = Array.from(selectedIds);
    selectedArray.forEach(id => {
        const checkbox = document.querySelector(`input[value="${id}"]`);
        if (checkbox) {
            switch (action) {
                case 'enable':
                    quickToggle(id, 'enabled', true);
                    break;
                case 'disable':
                    quickToggle(id, 'enabled', false);
                    break;
                case 'show':
                    quickToggle(id, 'hidden', false);
                    break;
                case 'hide':
                    quickToggle(id, 'hidden', true);
                    break;
                case 'delete':
                    deleteDescriptor(id, 'Selected descriptor');
                    break;
            }
        }
    });
}

// Group Functions
function groupSelected() {
    if (selectedIds.size === 0) {
        showToast('Please select at least one descriptor to group', 'warning');
        return;
    }
    
    document.getElementById('groupModal').classList.remove('hidden');
}

function closeGroupModal() {
    document.getElementById('groupModal').classList.add('hidden');
    document.getElementById('groupName').value = '';
}

function createGroup() {
    const groupName = document.getElementById('groupName').value.trim();
    if (!groupName) {
        showToast('Please enter a group name', 'warning');
        return;
    }
    
    const selectedArray = Array.from(selectedIds);
    
    const formData = new FormData();
    formData.append('action', 'group_descriptors');
    formData.append('descriptor_ids', JSON.stringify(selectedArray));
    formData.append('group_name', groupName);

    fetch(window.location.href, {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showToast('Group created successfully', 'success');
            closeGroupModal();
            setTimeout(() => location.reload(), 1000);
        } else {
            showToast('Failed to create group: ' + (data.error || 'Unknown error'), 'error');
        }
    })
    .catch(error => {
        showToast('Network error: ' + error.message, 'error');
    });
}

function toggleGroup(groupName) {
    const groupContent = document.getElementById(`group-${groupName}`);
    const toggleIcon = document.querySelector(`[onclick="toggleGroup('${groupName}')"] i`);
    
    if (groupContent.style.display === 'none') {
        groupContent.style.display = 'block';
        toggleIcon.className = 'fas fa-chevron-down group-toggle';
    } else {
        groupContent.style.display = 'none';
        toggleIcon.className = 'fas fa-chevron-right group-toggle';
    }
}

// Utility Functions
function deleteDescriptor(descriptorId, descriptorName) {
    if (!confirm(`Delete descriptor "${descriptorName}"?`)) {
        return;
    }
    
    // Implementation would go here - similar to existing delete functionality
    showToast('Delete functionality to be implemented', 'info');
}

function duplicateDescriptor(descriptorId) {
    // Implementation would go here - similar to existing duplicate functionality
    showToast('Duplicate functionality to be implemented', 'info');
}

function showToast(message, type = 'info') {
    // Create toast notification
    const toast = document.createElement('div');
    const bgColor = type === 'success' ? 'bg-green-500' : 
                   type === 'error' ? 'bg-red-500' : 
                   type === 'warning' ? 'bg-yellow-500' : 'bg-blue-500';
    
    toast.className = `fixed top-4 right-4 ${bgColor} text-white px-6 py-3 rounded-lg shadow-lg z-50 transform transition-transform duration-300 translate-x-full`;
    toast.textContent = message;
    
    document.body.appendChild(toast);
    
    // Slide in
    setTimeout(() => {
        toast.classList.remove('translate-x-full');
    }, 100);
    
    // Remove after 3 seconds
    setTimeout(() => {
        toast.classList.add('translate-x-full');
        setTimeout(() => {
            if (document.body.contains(toast)) {
                document.body.removeChild(toast);
            }
        }, 300);
    }, 3000);
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    updateSelection();
    
    // Initialize group toggle states
    const groupContents = document.querySelectorAll('.group-content');
    groupContents.forEach(content => {
        content.style.display = 'block'; // Start expanded
    });
});

// Make data available to JavaScript for enhanced functionality
const descriptors = <?= json_encode($descriptors) ?>;
const dealsLookup = <?= json_encode($dealsLookup) ?>;
const insuranceLookup = <?= json_encode($insuranceLookup) ?>;
const unitTypesLookup = <?= json_encode($unitTypesLookup) ?>;

// Keyboard shortcuts
document.addEventListener('keydown', function(e) {
    // Ctrl/Cmd + A to select all
    if ((e.ctrlKey || e.metaKey) && e.key === 'a' && e.target.tagName !== 'INPUT') {
        e.preventDefault();
        const selectAllCheckbox = document.getElementById('selectAll');
        if (selectAllCheckbox) {
            selectAllCheckbox.checked = true;
            toggleSelectAll();
        }
    }
    
    // Escape to clear selection
    if (e.key === 'Escape') {
        const selectAllCheckbox = document.getElementById('selectAll');
        if (selectAllCheckbox) {
            selectAllCheckbox.checked = false;
            toggleSelectAll();
        }
        closeGroupModal();
    }
    
    // Delete key to delete selected
    if (e.key === 'Delete' && selectedIds.size > 0) {
        batchAction('delete');
    }
});

// Export functionality
function exportData(format = 'csv') {
    const selectedData = descriptors.filter(desc => 
        selectedIds.size === 0 || selectedIds.has(desc._id)
    );
    
    if (format === 'csv') {
        const csv = convertToCSV(selectedData);
        downloadFile(csv, 'descriptors.csv', 'text/csv');
    } else if (format === 'json') {
        const json = JSON.stringify(selectedData, null, 2);
        downloadFile(json, 'descriptors.json', 'application/json');
    }
}

function convertToCSV(data) {
    const headers = ['Name', 'Description', 'Enabled', 'Visible', 'Carousel', 'Position', 'Inventory Total', 'Availability %'];
    const csvContent = [
        headers.join(','),
        ...data.map(desc => [
            `"${desc.name || ''}"`,
            `"${desc.description || ''}"`,
            desc.enabled ? 'Yes' : 'No',
            !desc.hidden ? 'Yes' : 'No',
            desc.useForCarousel ? 'Yes' : 'No',
            desc.ordinalPosition || 0,
            desc.inventory?.total || 0,
            desc.inventory?.availability || 0
        ].join(','))
    ].join('\n');
    
    return csvContent;
}

function downloadFile(content, filename, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
}

// Performance monitoring
function trackPerformance() {
    const perfData = {
        descriptorCount: descriptors.length,
        renderTime: performance.now(),
        memoryUsage: performance.memory ? {
            used: Math.round(performance.memory.usedJSHeapSize / 1024 / 1024),
            total: Math.round(performance.memory.totalJSHeapSize / 1024 / 1024)
        } : null
    };
    
    console.log('Performance data:', perfData);
}

// Initialize performance tracking
trackPerformance();
</script>

</body>
</html>