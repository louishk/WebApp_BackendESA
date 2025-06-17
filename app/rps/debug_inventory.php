<?php
/**
 * Enhanced Inventory Debug Page - Shows exact matching results
 * Save this as debug_inventory.php in your main directory
 */

session_start();

require_once 'config.php';
require_once 'RapidStorAPI.php';
require_once 'DataLoader.php';
require_once 'InventoryManager.php';

// Get parameters
$jwtToken = $_SESSION['jwt_token'] ?? '';
$selectedLocation = $_GET['location'] ?? 'L004';
$debug = true;

if (empty($jwtToken)) {
    die('Please set your JWT token in the main app first');
}

$api = new RapidStorAPI($jwtToken, $debug);

?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Enhanced Inventory Matching Debug</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
</head>
<body class="bg-gray-50 min-h-screen p-6">

<div class="max-w-7xl mx-auto">
    <div class="bg-white rounded-lg shadow-lg p-6">
        <h1 class="text-2xl font-bold mb-6 flex items-center gap-2">
            <i class="fas fa-bug text-red-500"></i>
            Enhanced Inventory Matching Debug - <?= $selectedLocation ?>
        </h1>

        <?php
        try {
            echo "<div class='mb-6 p-4 bg-blue-50 border border-blue-200 rounded'>";
            echo "<h3 class='font-semibold text-blue-900 mb-2'>Loading Data with Exact Matching...</h3>";

            $dataLoader = new DataLoader($api, $selectedLocation, $debug);

            // Capture debug output
            ob_start();
            $data = $dataLoader->loadAllData();
            $debugOutput = ob_get_clean();

            echo "<p class='text-blue-700'>Data loaded successfully!</p>";
            echo "</div>";

            // Statistics
            echo "<div class='grid grid-cols-1 md:grid-cols-5 gap-4 mb-6'>";
            echo "<div class='bg-blue-100 p-4 rounded'>";
            echo "<h4 class='font-semibold text-blue-900'>Descriptors</h4>";
            echo "<p class='text-2xl font-bold text-blue-700'>" . count($data['descriptors']) . "</p>";
            echo "</div>";

            echo "<div class='bg-green-100 p-4 rounded'>";
            echo "<h4 class='font-semibold text-green-900'>Unit Types</h4>";
            echo "<p class='text-2xl font-bold text-green-700'>" . count($data['unitTypes']) . "</p>";
            echo "</div>";

            echo "<div class='bg-emerald-100 p-4 rounded'>";
            echo "<h4 class='font-semibold text-emerald-900'>With Inventory</h4>";
            $withInventory = count(array_filter($data['descriptors'], function($d) { return $d['inventory']['total'] > 0; }));
            echo "<p class='text-2xl font-bold text-emerald-700'>{$withInventory}</p>";
            echo "</div>";

            echo "<div class='bg-red-100 p-4 rounded'>";
            echo "<h4 class='font-semibold text-red-900'>Without Inventory</h4>";
            $withoutInventory = count($data['descriptors']) - $withInventory;
            echo "<p class='text-2xl font-bold text-red-700'>{$withoutInventory}</p>";
            echo "</div>";

            echo "<div class='bg-yellow-100 p-4 rounded'>";
            echo "<h4 class='font-semibold text-yellow-900'>Match Rate</h4>";
            $matchRate = count($data['descriptors']) > 0 ? round(($withInventory / count($data['descriptors'])) * 100, 1) : 0;
            echo "<p class='text-2xl font-bold text-yellow-700'>{$matchRate}%</p>";
            echo "</div>";
            echo "</div>";

            // Detailed Analysis by Descriptor
            echo "<div class='mb-6'>";
            echo "<h3 class='text-lg font-semibold mb-3'>Detailed Descriptor Analysis</h3>";

            foreach ($data['descriptors'] as $descriptor) {
                $name = $descriptor['name'] ?? 'Unknown';
                $keywords = $descriptor['criteria']['include']['keywords'] ?? [];
                $inventory = $descriptor['inventory'] ?? ['total' => 0, 'vacant' => 0, 'matched_unit_types' => []];
                $matches = $inventory['matched_unit_types'] ?? [];

                // Card for each descriptor
                $cardClass = $inventory['total'] > 0 ? 'border-green-200 bg-green-50' : 'border-red-200 bg-red-50';
                echo "<div class='mb-4 p-4 rounded border {$cardClass}'>";

                // Header
                echo "<div class='flex justify-between items-start mb-3'>";
                echo "<div>";
                echo "<h4 class='font-semibold text-lg'>" . htmlspecialchars($name) . "</h4>";
                if ($inventory['total'] > 0) {
                    echo "<div class='text-green-600 font-medium'>✅ {$inventory['total']} total units, {$inventory['vacant']} vacant ({$inventory['availability']}% available)</div>";
                } else {
                    echo "<div class='text-red-600 font-medium'>❌ No inventory found</div>";
                }
                echo "</div>";
                echo "</div>";

                // Keywords
                echo "<div class='mb-3'>";
                echo "<div class='text-sm font-medium text-gray-700 mb-1'>Keywords from criteria.include.keywords:</div>";
                if (!empty($keywords)) {
                    echo "<div class='grid grid-cols-1 lg:grid-cols-2 gap-2'>";
                    foreach ($keywords as $keyword) {
                        // Clean the keyword for display
                        $cleanKeyword = stripslashes($keyword);

                        // Check if this keyword has exact matches
                        $hasExactMatch = false;
                        foreach ($data['unitTypes'] as $unitType) {
                            if (($unitType['sTypeName'] ?? '') === $cleanKeyword) {
                                $hasExactMatch = true;
                                break;
                            }
                        }

                        $keywordClass = $hasExactMatch ? 'bg-green-100 border-green-300 text-green-800' : 'bg-red-100 border-red-300 text-red-800';
                        $icon = $hasExactMatch ? '✅' : '❌';

                        echo "<div class='font-mono text-xs p-2 border rounded {$keywordClass}'>";
                        echo "{$icon} " . htmlspecialchars($cleanKeyword);
                        echo "</div>";
                    }
                    echo "</div>";
                } else {
                    echo "<div class='text-red-600 text-sm'>No keywords found!</div>";
                }
                echo "</div>";

                // Matches found
                if (!empty($matches)) {
                    echo "<div class='mb-3'>";
                    echo "<div class='text-sm font-medium text-gray-700 mb-1'>Exact matches found:</div>";
                    echo "<div class='grid grid-cols-1 lg:grid-cols-2 gap-2'>";
                    foreach ($matches as $match) {
                        echo "<div class='bg-white p-2 border border-green-200 rounded text-sm'>";
                        echo "<div class='font-mono text-xs text-gray-600'>" . htmlspecialchars($match['type_name']) . "</div>";
                        echo "<div class='text-green-600'>Total: {$match['total']}, Vacant: {$match['vacant']}</div>";
                        echo "<div class='text-gray-500 text-xs'>Floors: " . implode(', ', $match['floors']) . "</div>";
                        echo "</div>";
                    }
                    echo "</div>";
                    echo "</div>";
                }

                // If no matches, suggest potential issues
                if ($inventory['total'] == 0 && !empty($keywords)) {
                    echo "<div class='bg-yellow-50 border border-yellow-200 p-3 rounded'>";
                    echo "<div class='text-sm font-medium text-yellow-800 mb-2'>💡 Troubleshooting suggestions:</div>";

                    // Check for similar type names
                    $suggestions = [];
                    foreach ($keywords as $keyword) {
                        $cleanKeyword = stripslashes($keyword);

                        // Find type names that contain parts of this keyword
                        $similarTypeNames = [];
                        foreach ($data['unitTypes'] as $unitType) {
                            $typeName = $unitType['sTypeName'] ?? '';

                            // Check if they share significant parts
                            $keywordParts = preg_split('/[\/\-\s_]+/', strtolower($cleanKeyword));
                            $typeNameParts = preg_split('/[\/\-\s_]+/', strtolower($typeName));
                            $commonParts = array_intersect($keywordParts, $typeNameParts);

                            if (count($commonParts) >= 2) { // At least 2 common parts
                                $similarTypeNames[] = $typeName;
                            }
                        }

                        if (!empty($similarTypeNames)) {
                            $suggestions[] = "For keyword '{$cleanKeyword}', similar type names found: " .
                                implode(', ', array_slice(array_unique($similarTypeNames), 0, 3));
                        }
                    }

                    if (!empty($suggestions)) {
                        foreach ($suggestions as $suggestion) {
                            echo "<div class='text-xs text-yellow-700'>• " . htmlspecialchars($suggestion) . "</div>";
                        }
                    } else {
                        echo "<div class='text-xs text-yellow-700'>• No similar type names found. Check if keywords exactly match available sTypeNames.</div>";
                    }
                    echo "</div>";
                }

                echo "</div>";
            }
            echo "</div>";

            // All available sTypeNames for reference
            echo "<div class='mb-6'>";
            echo "<h3 class='text-lg font-semibold mb-3'>All Available sTypeNames (for keyword reference)</h3>";
            echo "<div class='bg-gray-50 p-4 rounded border max-h-96 overflow-y-auto'>";

            $allTypeNames = array_unique(array_map(function($ut) { return $ut['sTypeName'] ?? ''; }, $data['unitTypes']));
            $allTypeNames = array_filter($allTypeNames); // Remove empty
            sort($allTypeNames);

            echo "<div class='grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-1'>";
            foreach ($allTypeNames as $typeName) {
                echo "<div class='font-mono text-xs p-1 bg-white border border-gray-200 rounded'>" . htmlspecialchars($typeName) . "</div>";
            }
            echo "</div>";
            echo "</div>";
            echo "</div>";

            // Debug log (condensed)
            if ($debugOutput) {
                echo "<div class='mb-6'>";
                echo "<details class='bg-gray-50 border border-gray-200 rounded'>";
                echo "<summary class='p-3 cursor-pointer font-medium'>🔍 View Debug Log</summary>";
                echo "<div class='p-3 border-t'>";
                echo "<pre class='text-xs text-gray-700 overflow-auto max-h-64'>" . htmlspecialchars($debugOutput) . "</pre>";
                echo "</div>";
                echo "</details>";
                echo "</div>";
            }

        } catch (Exception $e) {
            echo "<div class='p-4 bg-red-50 border border-red-200 rounded'>";
            echo "<h3 class='font-semibold text-red-900'>Error:</h3>";
            echo "<p class='text-red-700'>" . htmlspecialchars($e->getMessage()) . "</p>";
            echo "</div>";
        }
        ?>

        <div class="mt-6 p-4 bg-blue-50 border border-blue-200 rounded">
            <h3 class="font-semibold text-blue-900 mb-2">Fixed Issues:</h3>
            <ul class="text-blue-700 text-sm space-y-1">
                <li>✅ <strong>Exact matching only</strong> - No more partial matches</li>
                <li>✅ <strong>Keyword cleaning</strong> - Removes escape slashes from JSON</li>
                <li>✅ <strong>Precise inventory</strong> - Only includes specified unit types</li>
                <li>✅ <strong>Clear debugging</strong> - Shows exactly which keywords match</li>
            </ul>
        </div>

        <div class="mt-4 text-center">
            <a href="index.php" class="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded">
                Back to Main App
            </a>
        </div>
    </div>
</div>

</body>
</html>