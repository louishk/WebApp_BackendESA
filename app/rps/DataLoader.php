<?php
require_once 'config.php';
require_once 'RapidStorAPI.php';
require_once 'InventoryManager.php';

/**
 * Data Loader for RapidStor application
 * Enhanced with better inventory debugging and keyword matching
 */
class DataLoader
{
    private $api;
    private $location;
    private $debug;
    private $inventoryManager;

    public function __construct($api, $location, $debug = false)
    {
        $this->api = $api;
        $this->location = $location;
        $this->debug = $debug;
    }

    private function log($message)
    {
        if ($this->debug) {
            error_log("[DataLoader] " . $message);
        }
    }

    public function loadAllData()
    {
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
            'errors' => [],
            'stats' => []
        ];

        $this->log("Starting data load for location: {$this->location}");

        // Load unit types first (needed for inventory calculations)
        $data['unitTypes'] = $this->loadUnitTypes();
        $data['lookups']['unitTypes'] = $this->buildLookup($data['unitTypes']);

        // Initialize inventory manager with unit types
        $this->inventoryManager = new InventoryManager($data['unitTypes'], $this->debug);

        // Debug unit type information
        if ($this->debug) {
            $this->debugUnitTypes($data['unitTypes']);
        }

        // Load descriptors
        $data['descriptors'] = $this->loadDescriptors();

        // Calculate inventory for descriptors
        $this->calculateInventoryForDescriptors($data['descriptors']);

        // Load deals and insurance
        $data['deals'] = $this->loadDeals();
        $data['insurance'] = $this->loadInsurance();

        // Build lookups
        $data['lookups']['deals'] = $this->buildLookup($data['deals']);
        $data['lookups']['insurance'] = $this->buildLookup($data['insurance']);

        // Generate statistics
        $data['stats'] = $this->generateStats($data);

        $this->log("Data load complete. Loaded: " .
            count($data['descriptors']) . " descriptors, " .
            count($data['deals']) . " deals, " .
            count($data['insurance']) . " insurance, " .
            count($data['unitTypes']) . " unit types");

        return $data;
    }

    private function loadDescriptors()
    {
        $this->log("Loading descriptors...");

        try {
            $result = $this->api->getDescriptors($this->location);

            if ($result['status'] === 200) {
                $descriptors = $result['data']['data'] ?? [];
                if (empty($descriptors) && !empty($result['data'])) {
                    $descriptors = is_array($result['data']) ? $result['data'] : [];
                }

                $this->log("Successfully loaded " . count($descriptors) . " descriptors");

                // Debug first few descriptors
                if ($this->debug && !empty($descriptors)) {
                    $this->debugDescriptors(array_slice($descriptors, 0, 3));
                }

                return $descriptors;
            } else {
                $error = "Failed to load descriptors: " . $this->extractErrorMessage($result);
                $this->log($error);
                throw new Exception($error);
            }
        } catch (Exception $e) {
            $this->log("Exception loading descriptors: " . $e->getMessage());
            throw $e;
        }
    }

    private function loadDeals()
    {
        $this->log("Loading deals...");

        try {
            $result = $this->api->getDeals($this->location);

            if ($result['status'] === 200) {
                $deals = $result['data'] ?? [];
                if (isset($deals['data'])) {
                    $deals = $deals['data'];
                }

                $this->log("Successfully loaded " . count($deals) . " deals");
                return $deals;
            } else {
                $this->log("Failed to load deals: " . $this->extractErrorMessage($result));
                return []; // Non-critical, return empty array
            }
        } catch (Exception $e) {
            $this->log("Exception loading deals: " . $e->getMessage());
            return []; // Non-critical, return empty array
        }
    }

    private function loadInsurance()
    {
        $this->log("Loading insurance...");

        try {
            $result = $this->api->getInsurance($this->location);

            if ($result['status'] === 200) {
                $insurance = $result['data'] ?? [];
                if (isset($insurance['data'])) {
                    $insurance = $insurance['data'];
                }

                $this->log("Successfully loaded " . count($insurance) . " insurance options");
                return $insurance;
            } else {
                $this->log("Failed to load insurance: " . $this->extractErrorMessage($result));
                return []; // Non-critical, return empty array
            }
        } catch (Exception $e) {
            $this->log("Exception loading insurance: " . $e->getMessage());
            return []; // Non-critical, return empty array
        }
    }

    private function loadUnitTypes()
    {
        $this->log("Loading unit types...");

        try {
            $result = $this->api->getUnitTypes($this->location);
            $this->log("Unit types API response status: " . $result['status']);

            if ($result['status'] === 200) {
                $unitTypes = $result['data'] ?? [];
                if (isset($unitTypes['data'])) {
                    $unitTypes = $unitTypes['data'];
                }

                $this->log("Successfully loaded " . count($unitTypes) . " unit types");

                return $unitTypes;
            } else {
                $this->log("Failed to load unit types: " . $this->extractErrorMessage($result));
                return []; // Return empty array, will affect inventory calculations
            }
        } catch (Exception $e) {
            $this->log("Exception loading unit types: " . $e->getMessage());
            return []; // Return empty array, will affect inventory calculations
        }
    }

    private function calculateInventoryForDescriptors(&$descriptors)
    {
        $this->log("Calculating inventory for descriptors...");

        $successCount = 0;
        $errorCount = 0;

        foreach ($descriptors as &$descriptor) {
            try {
                // Debug specific descriptors if enabled
                if ($this->debug) {
                    $this->inventoryManager->debugDescriptorMatching($descriptor);
                }

                $descriptor['inventory'] = $this->inventoryManager->calculateInventory($descriptor);

                if ($descriptor['inventory']['total'] > 0) {
                    $successCount++;
                } else {
                    $errorCount++;
                    if ($this->debug) {
                        $this->log("No inventory found for descriptor: " . ($descriptor['name'] ?? 'Unknown'));
                    }
                }
            } catch (Exception $e) {
                $errorCount++;
                $this->log("Error calculating inventory for descriptor " . ($descriptor['name'] ?? 'Unknown') . ": " . $e->getMessage());
                $descriptor['inventory'] = [
                    'total' => 0,
                    'occupied' => 0,
                    'reserved' => 0,
                    'vacant' => 0,
                    'availability' => 0,
                    'matched_unit_types' => [],
                    'error' => $e->getMessage()
                ];
            }
        }

        $this->log("Inventory calculation complete:");
        $this->log("- Total descriptors: " . count($descriptors));
        $this->log("- With inventory: " . $successCount);
        $this->log("- Without inventory: " . $errorCount);

        if ($this->debug && $successCount > 0) {
            // Log some successful examples
            $successfulDescriptors = array_filter($descriptors, function($desc) {
                return $desc['inventory']['total'] > 0;
            });

            foreach (array_slice($successfulDescriptors, 0, 3) as $desc) {
                $this->log("Success example: '{$desc['name']}' -> Total: {$desc['inventory']['total']}, Available: {$desc['inventory']['availability']}%");
            }
        }
    }

    private function debugUnitTypes($unitTypes)
    {
        if (empty($unitTypes)) {
            $this->log("WARNING: No unit types loaded!");
            return;
        }

        $this->log("=== UNIT TYPES DEBUG ===");
        $this->log("Total unit types: " . count($unitTypes));

        // Sample unit types
        $sample = array_slice($unitTypes, 0, 5);
        foreach ($sample as $unitType) {
            $this->log("Unit Type: " . ($unitType['sTypeName'] ?? 'N/A') .
                " | Floor: " . ($unitType['iFloor'] ?? 'N/A') .
                " | Total Units: " . ($unitType['iTotalUnits'] ?? 0) .
                " | Vacant: " . ($unitType['iTotalVacant'] ?? 0));
        }

        // Group by type names
        $typeNames = [];
        foreach ($unitTypes as $unitType) {
            if (isset($unitType['sTypeName'])) {
                $typeName = $unitType['sTypeName'];
                if (!isset($typeNames[$typeName])) {
                    $typeNames[$typeName] = 0;
                }
                $typeNames[$typeName]++;
            }
        }

        $this->log("Unique type names: " . count($typeNames));
        $this->log("Sample type names: " . implode(', ', array_slice(array_keys($typeNames), 0, 10)));
        $this->log("=== END UNIT TYPES DEBUG ===");
    }

    private function debugDescriptors($descriptors)
    {
        $this->log("=== DESCRIPTORS DEBUG ===");

        foreach ($descriptors as $descriptor) {
            $name = $descriptor['name'] ?? 'Unknown';
            $keywords = [];

            if (isset($descriptor['criteria']['include']['keywords'])) {
                $keywords = $descriptor['criteria']['include']['keywords'];
            }

            $this->log("Descriptor: '{$name}' | Keywords: " . json_encode($keywords));

            if (isset($descriptor['criteria']['include']['sizes']) && !empty($descriptor['criteria']['include']['sizes'])) {
                $this->log("  - Legacy sizes: " . json_encode($descriptor['criteria']['include']['sizes']));
            }
        }

        $this->log("=== END DESCRIPTORS DEBUG ===");
    }

    private function buildLookup($items)
    {
        $lookup = [];
        foreach ($items as $item) {
            if (isset($item['_id'])) {
                $lookup[$item['_id']] = $item;
            }
        }
        return $lookup;
    }

    private function extractErrorMessage($result)
    {
        if (isset($result['data']['error'])) {
            return $result['data']['error'];
        } elseif (isset($result['data']['message'])) {
            return $result['data']['message'];
        } elseif (isset($result['data']['msg'])) {
            return $result['data']['msg'];
        } elseif (!empty($result['raw'])) {
            return "HTTP {$result['status']}: " . substr($result['raw'], 0, 200);
        } else {
            return 'Unknown error';
        }
    }

    private function generateStats($data)
    {
        $stats = [
            'counts' => [
                'descriptors' => count($data['descriptors']),
                'deals' => count($data['deals']),
                'insurance' => count($data['insurance']),
                'unitTypes' => count($data['unitTypes'])
            ],
            'inventory' => $this->inventoryManager ? $this->inventoryManager->getInventoryStats($data['descriptors']) : [],
            'descriptors' => [
                'enabled' => 0,
                'disabled' => 0,
                'visible' => 0,
                'hidden' => 0,
                'carousel' => 0,
                'with_inventory' => 0,
                'without_inventory' => 0
            ]
        ];

        // Calculate descriptor stats
        foreach ($data['descriptors'] as $descriptor) {
            $stats['descriptors']['enabled'] += $descriptor['enabled'] ? 1 : 0;
            $stats['descriptors']['disabled'] += !$descriptor['enabled'] ? 1 : 0;
            $stats['descriptors']['visible'] += !($descriptor['hidden'] ?? false) ? 1 : 0;
            $stats['descriptors']['hidden'] += ($descriptor['hidden'] ?? false) ? 1 : 0;
            $stats['descriptors']['carousel'] += ($descriptor['useForCarousel'] ?? false) ? 1 : 0;

            if (isset($descriptor['inventory']) && $descriptor['inventory']['total'] > 0) {
                $stats['descriptors']['with_inventory']++;
            } else {
                $stats['descriptors']['without_inventory']++;
            }
        }

        return $stats;
    }

    public function filterDescriptors($descriptors, $searchTerm)
    {
        if (empty($searchTerm)) {
            return $descriptors;
        }

        $this->log("Filtering descriptors with search term: {$searchTerm}");

        $filtered = array_filter($descriptors, function($desc) use ($searchTerm) {
            $searchLower = strtolower($searchTerm);
            return strpos(strtolower($desc['name'] ?? ''), $searchLower) !== false ||
                strpos(strtolower($desc['description'] ?? ''), $searchLower) !== false ||
                strpos(strtolower($desc['specialText'] ?? ''), $searchLower) !== false;
        });

        $this->log("Filtered to " . count($filtered) . " descriptors");
        return $filtered;
    }

    public function sortDescriptors($descriptors, $sortBy, $sortOrder)
    {
        $this->log("Sorting descriptors by {$sortBy} ({$sortOrder})");

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

        return $descriptors;
    }

    public function groupDescriptors($descriptors)
    {
        $this->log("Grouping descriptors by size pattern");

        $grouped = [];

        foreach ($descriptors as $descriptor) {
            $sizeName = 'Ungrouped';

            // Try to extract size from name
            if (preg_match('/(\d+(?:\.\d+)?(?:-\d+(?:\.\d+)?)?)\s*(?:sq\s*ft|sqft|square\s*feet)/i', $descriptor['name'], $matches)) {
                $sizeName = $matches[1] . ' sq ft';
            }
            // Try to extract dimensions like "8x10"
            elseif (preg_match('/(\d+(?:\.\d+)?)\s*x\s*(\d+(?:\.\d+)?)/i', $descriptor['name'], $matches)) {
                $sizeName = $matches[1] . 'x' . $matches[2];
            }

            if (!isset($grouped[$sizeName])) {
                $grouped[$sizeName] = [];
            }
            $grouped[$sizeName][] = $descriptor;
        }

        // Sort groups by name
        ksort($grouped);

        $this->log("Created " . count($grouped) . " groups");
        return $grouped;
    }

    public function testEndpoint($endpoint, $params = [])
    {
        $this->log("Testing endpoint: {$endpoint}");

        try {
            $result = $this->api->testEndpoint($endpoint, $params);

            return [
                'success' => $result['status'] === 200,
                'status' => $result['status'],
                'url' => $result['url'],
                'data' => $result['data'],
                'raw' => substr($result['raw'] ?? '', 0, 1000)
            ];
        } catch (Exception $e) {
            return [
                'success' => false,
                'error' => $e->getMessage(),
                'endpoint' => $endpoint
            ];
        }
    }

    public function getInventoryManager()
    {
        return $this->inventoryManager;
    }

    /**
     * Debug method to analyze inventory matching issues
     */
    public function debugInventoryMatching($descriptorId = null)
    {
        if (!$this->debug) {
            return "Debug mode not enabled";
        }

        $output = [];
        $output[] = "=== INVENTORY MATCHING DEBUG ===";

        if (!$this->inventoryManager) {
            $output[] = "ERROR: Inventory manager not initialized";
            return implode("\n", $output);
        }

        // Get unit types by type name
        $unitTypesByTypeName = $this->inventoryManager->getUnitTypesByTypeName();
        $output[] = "Available unit type names: " . count($unitTypesByTypeName);

        foreach (array_slice(array_keys($unitTypesByTypeName), 0, 10) as $typeName) {
            $count = count($unitTypesByTypeName[$typeName]);
            $output[] = "  - {$typeName} ({$count} variants)";
        }

        return implode("\n", $output);
    }
}
?>