<?php
require_once 'config.php';
require_once 'RapidStorAPI.php';

/**
 * AJAX Request Handler for RapidStor operations - Optimized for Performance
 */
class AjaxHandler
{
    private $api;
    private $location;

    public function __construct($api, $location)
    {
        $this->api = $api;
        $this->location = $location;
    }

    public function handleRequest($action, $postData)
    {
        header('Content-Type: application/json');

        try {
            switch ($action) {
                case 'quick_toggle':
                    return $this->handleQuickToggle($postData);

                case 'reorder_descriptors':
                    return $this->handleReorderDescriptorsOptimized($postData);

                case 'group_descriptors':
                    return $this->handleGroupDescriptors($postData);

                case 'batch_update':
                    return $this->handleBatchUpdate($postData);

                default:
                    throw new Exception("Unknown AJAX action: {$action}");
            }
        } catch (Exception $e) {
            return [
                'success' => false,
                'error' => $e->getMessage(),
                'action' => $action
            ];
        }
    }

    private function handleQuickToggle($postData)
    {
        $descriptorId = $postData['descriptor_id'] ?? '';
        $field = $postData['field'] ?? '';
        $value = ($postData['value'] ?? 'false') === 'true';

        if (empty($descriptorId) || empty($field)) {
            throw new Exception("Missing required parameters: descriptor_id and field");
        }

        // Validate field
        $allowedFields = ['enabled', 'hidden', 'useForCarousel'];
        if (!in_array($field, $allowedFields)) {
            throw new Exception("Invalid field: {$field}");
        }

        // Get current descriptor data
        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200) {
            throw new Exception("Failed to load descriptors: " . ($descriptorsResult['data']['error'] ?? 'Unknown error'));
        }

        $allDescriptors = $descriptorsResult['data']['data'] ?? [];
        $descriptor = null;

        foreach ($allDescriptors as $desc) {
            if ($desc['_id'] === $descriptorId) {
                $descriptor = $desc;
                break;
            }
        }

        if (!$descriptor) {
            throw new Exception("Descriptor not found: {$descriptorId}");
        }

        // Update the field
        $descriptor[$field] = $value;

        // Save the descriptor
        $result = $this->api->saveDescriptor($descriptor, $this->location);

        if ($result['status'] !== 200) {
            throw new Exception("Failed to save descriptor: " . ($result['data']['error'] ?? 'Unknown error'));
        }

        return [
            'success' => true,
            'message' => 'Field updated successfully',
            'field' => $field,
            'value' => $value,
            'descriptor_id' => $descriptorId
        ];
    }

    /**
     * Optimized reorder handler - processes in smaller batches and uses individual saves
     */
    private function handleReorderDescriptorsOptimized($postData)
    {
        // Increase execution time for this operation
        ini_set('max_execution_time', 120);

        $orderedIds = json_decode($postData['ordered_ids'] ?? '[]', true);

        if (empty($orderedIds)) {
            throw new Exception("No descriptor IDs provided for reordering");
        }

        // Limit the number of descriptors we process at once
        if (count($orderedIds) > 100) {
            throw new Exception("Too many descriptors to reorder at once. Please try with fewer items.");
        }

        // Get current descriptors
        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200) {
            throw new Exception("Failed to load descriptors for reordering");
        }

        $allDescriptors = $descriptorsResult['data']['data'] ?? [];
        $descriptorMap = [];

        // Create a map of ID -> descriptor
        foreach ($allDescriptors as $desc) {
            $descriptorMap[$desc['_id']] = $desc;
        }

        // Check if reordering is actually needed
        $changedDescriptors = [];
        foreach ($orderedIds as $index => $id) {
            if (isset($descriptorMap[$id])) {
                $newPosition = $index + 1; // 1-based indexing
                $currentPosition = $descriptorMap[$id]['ordinalPosition'] ?? 0;

                if ($currentPosition != $newPosition) {
                    $descriptor = $descriptorMap[$id];
                    $descriptor['ordinalPosition'] = $newPosition;
                    $changedDescriptors[] = $descriptor;
                }
            }
        }

        if (empty($changedDescriptors)) {
            return [
                'success' => true,
                'message' => 'No changes needed - descriptors already in correct order',
                'updated_count' => 0,
                'location' => $this->location
            ];
        }

        // Process in smaller batches to avoid timeouts
        $batchSize = 10; // Process 10 descriptors at a time
        $totalUpdated = 0;
        $errors = [];

        for ($i = 0; $i < count($changedDescriptors); $i += $batchSize) {
            $batch = array_slice($changedDescriptors, $i, $batchSize);

            try {
                // Try batch update first
                $result = $this->api->batchUpdate('save', $batch, $this->location);

                if ($result['status'] === 200) {
                    $totalUpdated += count($batch);
                } else {
                    // If batch fails, try individual saves
                    foreach ($batch as $descriptor) {
                        try {
                            $individualResult = $this->api->saveDescriptor($descriptor, $this->location);
                            if ($individualResult['status'] === 200) {
                                $totalUpdated++;
                            } else {
                                $errors[] = "Failed to update {$descriptor['name']}: " .
                                    ($individualResult['data']['error'] ?? 'Unknown error');
                            }
                        } catch (Exception $e) {
                            $errors[] = "Exception updating {$descriptor['name']}: " . $e->getMessage();
                        }
                    }
                }
            } catch (Exception $e) {
                // If batch completely fails, try individual saves
                foreach ($batch as $descriptor) {
                    try {
                        $individualResult = $this->api->saveDescriptor($descriptor, $this->location);
                        if ($individualResult['status'] === 200) {
                            $totalUpdated++;
                        } else {
                            $errors[] = "Failed to update {$descriptor['name']}: " .
                                ($individualResult['data']['error'] ?? 'Unknown error');
                        }
                    } catch (Exception $e) {
                        $errors[] = "Exception updating {$descriptor['name']}: " . $e->getMessage();
                    }
                }
            }

            // Small delay between batches to prevent overwhelming the API
            if ($i + $batchSize < count($changedDescriptors)) {
                usleep(100000); // 0.1 second delay
            }
        }

        $success = $totalUpdated > 0;
        $message = $success ?
            "Successfully updated {$totalUpdated} of " . count($changedDescriptors) . " descriptors" :
            "Failed to update any descriptors";

        if (!empty($errors)) {
            $message .= ". Errors: " . implode('; ', array_slice($errors, 0, 3));
            if (count($errors) > 3) {
                $message .= " and " . (count($errors) - 3) . " more...";
            }
        }

        return [
            'success' => $success,
            'message' => $message,
            'updated_count' => $totalUpdated,
            'total_attempted' => count($changedDescriptors),
            'errors' => $errors,
            'location' => $this->location,
            'new_order' => array_map(function($desc) {
                return [
                    'id' => $desc['_id'],
                    'name' => $desc['name'],
                    'position' => $desc['ordinalPosition']
                ];
            }, array_slice($changedDescriptors, 0, 10)) // Return first 10 for verification
        ];
    }

    /**
     * Legacy reorder handler (kept for backup)
     */
    private function handleReorderDescriptors($postData)
    {
        $orderedIds = json_decode($postData['ordered_ids'] ?? '[]', true);

        if (empty($orderedIds)) {
            throw new Exception("No descriptor IDs provided for reordering");
        }

        // Get current descriptors
        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200) {
            throw new Exception("Failed to load descriptors for reordering");
        }

        $allDescriptors = $descriptorsResult['data']['data'] ?? [];
        $descriptorMap = [];

        // Create a map of ID -> descriptor
        foreach ($allDescriptors as $desc) {
            $descriptorMap[$desc['_id']] = $desc;
        }

        // Update ordinal positions based on new order
        $updatedDescriptors = [];
        foreach ($orderedIds as $index => $id) {
            if (isset($descriptorMap[$id])) {
                $descriptor = $descriptorMap[$id];
                $descriptor['ordinalPosition'] = $index + 1; // 1-based indexing
                $updatedDescriptors[] = $descriptor;
            }
        }

        if (empty($updatedDescriptors)) {
            throw new Exception("No valid descriptors found for reordering");
        }

        // Use batchUpdate with 'save' operation to update the ordinal positions
        $result = $this->api->batchUpdate('save', $updatedDescriptors, $this->location);

        if ($result['status'] !== 200) {
            throw new Exception("Failed to update descriptor order: " . ($result['data']['error'] ?? 'Unknown error'));
        }

        return [
            'success' => true,
            'message' => 'Descriptor order updated successfully',
            'updated_count' => count($updatedDescriptors),
            'location' => $this->location,
            'new_order' => array_map(function($desc) {
                return [
                    'id' => $desc['_id'],
                    'name' => $desc['name'],
                    'position' => $desc['ordinalPosition']
                ];
            }, $updatedDescriptors)
        ];
    }

    private function handleGroupDescriptors($postData)
    {
        $descriptorIds = json_decode($postData['descriptor_ids'] ?? '[]', true);
        $groupName = trim($postData['group_name'] ?? '');

        if (empty($descriptorIds)) {
            throw new Exception("No descriptors selected for grouping");
        }

        if (empty($groupName)) {
            throw new Exception("Group name is required");
        }

        // For now, return a placeholder response
        // In the future, this could update a 'group' field on the descriptors
        return [
            'success' => true,
            'message' => 'Grouping feature is planned for future implementation',
            'group_name' => $groupName,
            'descriptor_count' => count($descriptorIds)
        ];
    }

    private function handleBatchUpdate($postData)
    {
        $operation = $postData['operation'] ?? '';
        $selectedIds = json_decode($postData['selected_ids'] ?? '[]', true);

        if (empty($operation)) {
            throw new Exception("Operation is required for batch update");
        }

        if (empty($selectedIds)) {
            throw new Exception("No descriptors selected for batch update");
        }

        // Get all descriptors
        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200) {
            throw new Exception("Failed to load descriptors for batch update");
        }

        $allDescriptors = $descriptorsResult['data']['data'] ?? [];
        $selectedDescriptors = array_filter($allDescriptors, function($desc) use ($selectedIds) {
            return in_array($desc['_id'], $selectedIds);
        });

        if (empty($selectedDescriptors)) {
            throw new Exception("No matching descriptors found for batch update");
        }

        // Apply the operation
        switch ($operation) {
            case 'enable':
                $selectedDescriptors = array_map(function($desc) {
                    $desc['enabled'] = true;
                    return $desc;
                }, $selectedDescriptors);
                $apiOperation = 'save';
                break;

            case 'disable':
                $selectedDescriptors = array_map(function($desc) {
                    $desc['enabled'] = false;
                    return $desc;
                }, $selectedDescriptors);
                $apiOperation = 'save';
                break;

            case 'show':
                $selectedDescriptors = array_map(function($desc) {
                    $desc['hidden'] = false;
                    return $desc;
                }, $selectedDescriptors);
                $apiOperation = 'save';
                break;

            case 'hide':
                $selectedDescriptors = array_map(function($desc) {
                    $desc['hidden'] = true;
                    return $desc;
                }, $selectedDescriptors);
                $apiOperation = 'save';
                break;

            case 'delete':
                $apiOperation = 'delete';
                break;

            default:
                throw new Exception("Unknown batch operation: {$operation}");
        }

        // Execute the batch operation
        $result = $this->api->batchUpdate($apiOperation, array_values($selectedDescriptors), $this->location);

        if ($result['status'] !== 200) {
            throw new Exception("Batch {$operation} failed: " . ($result['data']['error'] ?? 'Unknown error'));
        }

        return [
            'success' => true,
            'message' => "Batch {$operation} completed successfully",
            'operation' => $operation,
            'affected_count' => count($selectedDescriptors),
            'location' => $this->location
        ];
    }
}
?>