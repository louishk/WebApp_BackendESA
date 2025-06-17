<?php
/**
 * Enhanced Inventory Manager with EXACT keyword matching
 * Fixes the issue where partial matches were incorrectly included
 */
class InventoryManager
{
    private $unitTypesLookup;
    private $unitTypesByTypeName;
    private $debug;

    public function __construct($unitTypes = [], $debug = false)
    {
        $this->debug = $debug;
        $this->buildLookups($unitTypes);
    }

    private function log($message)
    {
        if ($this->debug) {
            error_log("[InventoryManager] " . $message);
        }
    }

    private function buildLookups($unitTypes)
    {
        $this->unitTypesLookup = [];
        $this->unitTypesByTypeName = [];

        foreach ($unitTypes as $unitType) {
            if (isset($unitType['_id'])) {
                $this->unitTypesLookup[$unitType['_id']] = $unitType;
            }

            // Group by sTypeName for keyword matching
            if (isset($unitType['sTypeName'])) {
                $typeName = $unitType['sTypeName'];
                if (!isset($this->unitTypesByTypeName[$typeName])) {
                    $this->unitTypesByTypeName[$typeName] = [];
                }
                $this->unitTypesByTypeName[$typeName][] = $unitType;
            }
        }

        $this->log("Built lookup with " . count($this->unitTypesLookup) . " unit types");
        $this->log("Built type name lookup with " . count($this->unitTypesByTypeName) . " unique type names");

        if ($this->debug) {
            $this->log("Sample type names: " . implode(', ', array_slice(array_keys($this->unitTypesByTypeName), 0, 5)));
        }
    }

    public function calculateInventory($descriptor)
    {
        $inventory = [
            'total' => 0,
            'occupied' => 0,
            'reserved' => 0,
            'vacant' => 0,
            'availability' => 0,
            'matched_unit_types' => []
        ];

        $descriptorName = $descriptor['name'] ?? 'Unknown';
        $this->log("Calculating inventory for descriptor: {$descriptorName}");

        // Get matching unit types using EXACT keyword matching
        $matchingUnitTypes = $this->getExactMatchingUnitTypes($descriptor);

        $this->log("Found " . count($matchingUnitTypes) . " exactly matching unit types for descriptor: {$descriptorName}");

        // Aggregate inventory from all matching unit types
        $typeNameGroups = [];

        foreach ($matchingUnitTypes as $unitType) {
            $typeName = $unitType['sTypeName'] ?? 'Unknown';

            if (!isset($typeNameGroups[$typeName])) {
                $typeNameGroups[$typeName] = [
                    'total' => 0,
                    'occupied' => 0,
                    'reserved' => 0,
                    'vacant' => 0,
                    'floors' => [],
                    'units' => []
                ];
            }

            // Aggregate data for this type name
            $typeNameGroups[$typeName]['total'] += intval($unitType['iTotalUnits'] ?? 0);
            $typeNameGroups[$typeName]['occupied'] += intval($unitType['iTotalOccupied'] ?? 0);
            $typeNameGroups[$typeName]['reserved'] += intval($unitType['iTotalReserved'] ?? 0);
            $typeNameGroups[$typeName]['vacant'] += intval($unitType['iTotalVacant'] ?? 0);
            $typeNameGroups[$typeName]['floors'][] = $unitType['iFloor'] ?? 'Unknown';
            $typeNameGroups[$typeName]['units'][] = $unitType;

            $this->log("Added unit type {$typeName} (Floor: " . ($unitType['iFloor'] ?? 'N/A') . ") - " .
                "Total: " . ($unitType['iTotalUnits'] ?? 0) . ", " .
                "Vacant: " . ($unitType['iTotalVacant'] ?? 0));
        }

        // Sum up all type name groups
        foreach ($typeNameGroups as $typeName => $group) {
            $inventory['total'] += $group['total'];
            $inventory['occupied'] += $group['occupied'];
            $inventory['reserved'] += $group['reserved'];
            $inventory['vacant'] += $group['vacant'];

            $inventory['matched_unit_types'][] = [
                'type_name' => $typeName,
                'total' => $group['total'],
                'vacant' => $group['vacant'],
                'occupied' => $group['occupied'],
                'reserved' => $group['reserved'],
                'floors' => array_unique($group['floors']),
                'unit_count' => count($group['units'])
            ];
        }

        $inventory['availability'] = $inventory['total'] > 0 ?
            round(($inventory['vacant'] / $inventory['total']) * 100, 1) : 0;

        $this->log("Final inventory for {$descriptorName}: " .
            "Total: {$inventory['total']}, " .
            "Vacant: {$inventory['vacant']}, " .
            "Availability: {$inventory['availability']}%");

        return $inventory;
    }

    /**
     * Get unit types that EXACTLY match the descriptor keywords
     * No partial matching - only exact sTypeName matches
     */
    private function getExactMatchingUnitTypes($descriptor)
    {
        $matchingUnitTypes = [];

        // Get keywords from descriptor criteria
        $keywords = $this->extractKeywordsFromDescriptor($descriptor);

        $this->log("Extracted keywords for exact matching: " . implode(', ', $keywords));

        if (empty($keywords)) {
            $this->log("No keywords found, no matches possible");
            return [];
        }

        // Only exact matches - no partial matching
        foreach ($keywords as $keyword) {
            $keyword = trim($keyword);
            if (empty($keyword)) continue;

            // Only exact match in unitTypesByTypeName
            if (isset($this->unitTypesByTypeName[$keyword])) {
                $matchingUnitTypes = array_merge($matchingUnitTypes, $this->unitTypesByTypeName[$keyword]);
                $this->log("EXACT match found for keyword: '{$keyword}' (" . count($this->unitTypesByTypeName[$keyword]) . " unit types)");
            } else {
                $this->log("NO exact match found for keyword: '{$keyword}'");

                // Debug: Show similar type names for troubleshooting
                if ($this->debug) {
                    $similarTypeNames = $this->findSimilarTypeNames($keyword);
                    if (!empty($similarTypeNames)) {
                        $this->log("Similar type names found: " . implode(', ', array_slice($similarTypeNames, 0, 3)));
                    }
                }
            }
        }

        // Remove duplicates based on unit type ID
        $uniqueUnitTypes = [];
        $seenIds = [];

        foreach ($matchingUnitTypes as $unitType) {
            $id = $unitType['_id'] ?? uniqid();
            if (!in_array($id, $seenIds)) {
                $uniqueUnitTypes[] = $unitType;
                $seenIds[] = $id;
            }
        }

        return $uniqueUnitTypes;
    }

    /**
     * Extract keywords from descriptor - only from criteria.include.keywords
     */
    private function extractKeywordsFromDescriptor($descriptor)
    {
        $keywords = [];

        // Primary source: criteria.include.keywords
        if (isset($descriptor['criteria']['include']['keywords']) && is_array($descriptor['criteria']['include']['keywords'])) {
            $keywords = $descriptor['criteria']['include']['keywords'];
            $this->log("Found keywords in criteria.include.keywords: " . json_encode($keywords));
        }

        // Legacy support: criteria.include.sizes (but only if keywords is empty)
        if (empty($keywords) && isset($descriptor['criteria']['include']['sizes']) && is_array($descriptor['criteria']['include']['sizes'])) {
            $keywords = $descriptor['criteria']['include']['sizes'];
            $this->log("Using legacy sizes as keywords: " . json_encode($keywords));
        }

        // Clean keywords - remove escape slashes and normalize
        $cleanedKeywords = [];
        foreach ($keywords as $keyword) {
            // Remove escape slashes that might come from JSON
            $cleaned = stripslashes(trim($keyword));
            if (!empty($cleaned)) {
                $cleanedKeywords[] = $cleaned;
            }
        }

        $this->log("Cleaned keywords: " . json_encode($cleanedKeywords));

        return array_unique($cleanedKeywords);
    }

    /**
     * Find similar type names for debugging purposes
     */
    private function findSimilarTypeNames($keyword)
    {
        $similar = [];
        $keywordLower = strtolower($keyword);

        foreach (array_keys($this->unitTypesByTypeName) as $typeName) {
            $typeNameLower = strtolower($typeName);

            // Check if they share common parts
            $keywordParts = preg_split('/[\/\-\s_]+/', $keywordLower);
            $typeNameParts = preg_split('/[\/\-\s_]+/', $typeNameLower);

            $commonParts = array_intersect($keywordParts, $typeNameParts);

            // If they share 2 or more parts, consider it similar
            if (count($commonParts) >= 2) {
                $similar[] = $typeName;
            }
        }

        return $similar;
    }

    public function getUnitTypesLookup()
    {
        return $this->unitTypesLookup;
    }

    public function getUnitTypesByTypeName()
    {
        return $this->unitTypesByTypeName;
    }

    public function getInventoryStats($descriptors)
    {
        $stats = [
            'total_descriptors' => count($descriptors),
            'descriptors_with_inventory' => 0,
            'total_units' => 0,
            'total_vacant' => 0,
            'average_availability' => 0
        ];

        $availabilities = [];

        foreach ($descriptors as $descriptor) {
            if (isset($descriptor['inventory'])) {
                $inv = $descriptor['inventory'];
                if ($inv['total'] > 0) {
                    $stats['descriptors_with_inventory']++;
                    $stats['total_units'] += $inv['total'];
                    $stats['total_vacant'] += $inv['vacant'];
                    $availabilities[] = $inv['availability'];
                }
            }
        }

        if (!empty($availabilities)) {
            $stats['average_availability'] = round(array_sum($availabilities) / count($availabilities), 1);
        }

        return $stats;
    }

    /**
     * Enhanced debug method to show exact matching details
     */
    public function debugDescriptorMatching($descriptor)
    {
        if (!$this->debug) return;

        $name = $descriptor['name'] ?? 'Unknown';
        $this->log("=== EXACT MATCH DEBUG: Descriptor '{$name}' ===");

        $keywords = $this->extractKeywordsFromDescriptor($descriptor);
        $this->log("Cleaned extracted keywords: " . json_encode($keywords));

        // Check each keyword individually
        foreach ($keywords as $keyword) {
            $this->log("Checking keyword: '{$keyword}'");

            if (isset($this->unitTypesByTypeName[$keyword])) {
                $matches = $this->unitTypesByTypeName[$keyword];
                $this->log("  ✅ EXACT MATCH found: " . count($matches) . " unit types");

                foreach ($matches as $match) {
                    $this->log("    - " . ($match['sTypeName'] ?? 'N/A') .
                        " (Floor: " . ($match['iFloor'] ?? 'N/A') .
                        ", Units: " . ($match['iTotalUnits'] ?? 0) .
                        ", Vacant: " . ($match['iTotalVacant'] ?? 0) . ")");
                }
            } else {
                $this->log("  ❌ NO exact match found for: '{$keyword}'");

                // Show what was available for debugging
                $similar = $this->findSimilarTypeNames($keyword);
                if (!empty($similar)) {
                    $this->log("    Similar type names: " . implode(', ', array_slice($similar, 0, 3)));
                }
            }
        }

        $this->log("=== END EXACT MATCH DEBUG ===");
    }

    /**
     * Debug method to show all available type names
     */
    public function debugAvailableTypeNames($maxShow = 50)
    {
        if (!$this->debug) return;

        $this->log("=== AVAILABLE TYPE NAMES DEBUG ===");
        $typeNames = array_keys($this->unitTypesByTypeName);
        $this->log("Total unique type names: " . count($typeNames));

        foreach (array_slice($typeNames, 0, $maxShow) as $typeName) {
            $count = count($this->unitTypesByTypeName[$typeName]);
            $this->log("  - '{$typeName}' ({$count} variants)");
        }

        if (count($typeNames) > $maxShow) {
            $this->log("  ... and " . (count($typeNames) - $maxShow) . " more");
        }

        $this->log("=== END AVAILABLE TYPE NAMES DEBUG ===");
    }
}
?>