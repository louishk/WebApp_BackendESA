<?php
/**
 * Enhanced Inventory Manager with Occupancy % and Keywords Info
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
    }

    public function calculateInventory($descriptor)
    {
        $inventory = [
            'total' => 0,
            'occupied' => 0,
            'reserved' => 0,
            'vacant' => 0,
            'availability' => 0,      // Keep for backward compatibility
            'occupancy' => 0,         // NEW: Occupancy percentage
            'matched_unit_types' => [],
            'keywords' => [],         // NEW: Keywords used for matching
            'matching_summary' => ''  // NEW: Summary for display
        ];

        $descriptorName = $descriptor['name'] ?? 'Unknown';
        $this->log("Calculating inventory for descriptor: {$descriptorName}");

        // Extract and store keywords
        $keywords = $this->extractKeywordsFromDescriptor($descriptor);
        $inventory['keywords'] = $keywords;

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
                "Occupied: " . ($unitType['iTotalOccupied'] ?? 0) . ", " .
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

        // Calculate percentages
        if ($inventory['total'] > 0) {
            $inventory['availability'] = round(($inventory['vacant'] / $inventory['total']) * 100, 1);
            $inventory['occupancy'] = round(($inventory['occupied'] / $inventory['total']) * 100, 1);
        } else {
            $inventory['availability'] = 0;
            $inventory['occupancy'] = 0;
        }

        // Create matching summary for display
        $inventory['matching_summary'] = $this->createMatchingSummary($keywords, $inventory['matched_unit_types']);

        $this->log("Final inventory for {$descriptorName}: " .
            "Total: {$inventory['total']}, " .
            "Occupied: {$inventory['occupied']} ({$inventory['occupancy']}%), " .
            "Vacant: {$inventory['vacant']} ({$inventory['availability']}%)");

        return $inventory;
    }

    /**
     * Create a summary of keywords and matching unit types for display
     */
    private function createMatchingSummary($keywords, $matchedUnitTypes)
    {
        if (empty($keywords) && empty($matchedUnitTypes)) {
            return 'No keywords or matches';
        }

        $summary = '';

        // Add keywords info
        if (!empty($keywords)) {
            $keywordCount = count($keywords);
            $summary .= "{$keywordCount} keyword" . ($keywordCount !== 1 ? 's' : '');
        }

        // Add matching unit types info
        if (!empty($matchedUnitTypes)) {
            $matchCount = count($matchedUnitTypes);
            if (!empty($summary)) $summary .= ', ';
            $summary .= "{$matchCount} match" . ($matchCount !== 1 ? 'es' : '');
        }

        return $summary;
    }

    /**
     * Get unit types that EXACTLY match the descriptor keywords
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
            'total_occupied' => 0,
            'average_availability' => 0,
            'average_occupancy' => 0     // NEW: Average occupancy
        ];

        $availabilities = [];
        $occupancies = [];

        foreach ($descriptors as $descriptor) {
            if (isset($descriptor['inventory'])) {
                $inv = $descriptor['inventory'];
                if ($inv['total'] > 0) {
                    $stats['descriptors_with_inventory']++;
                    $stats['total_units'] += $inv['total'];
                    $stats['total_vacant'] += $inv['vacant'];
                    $stats['total_occupied'] += $inv['occupied'];
                    $availabilities[] = $inv['availability'];
                    $occupancies[] = $inv['occupancy'];
                }
            }
        }

        if (!empty($availabilities)) {
            $stats['average_availability'] = round(array_sum($availabilities) / count($availabilities), 1);
        }

        if (!empty($occupancies)) {
            $stats['average_occupancy'] = round(array_sum($occupancies) / count($occupancies), 1);
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
                        ", Occupied: " . ($match['iTotalOccupied'] ?? 0) .
                        ", Vacant: " . ($match['iTotalVacant'] ?? 0) . ")");
                }
            } else {
                $this->log("  ❌ NO exact match found for: '{$keyword}'");
            }
        }

        $this->log("=== END EXACT MATCH DEBUG ===");
    }
}
?>