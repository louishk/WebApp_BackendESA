<?php

require_once 'config.php';
require_once 'RapidStorAPI.php';
require_once 'InventoryManager.php';

/**
 * Enhanced AJAX Request Handler for RapidStor operations
 * Includes batch actions, auto upsells, and smart operations
 */
class AjaxHandler
{
    private $api;
    private $location;
    private $debug;

    public function __construct(RapidStorAPI $api, string $location, bool $debug = false)
    {
        $this->api = $api;
        $this->location = $location;
        $this->debug = $debug;
    }

    /**
     * Main request handler - routes AJAX requests to appropriate methods
     */
    public function handleRequest(string $action, array $postData): array
    {
        header('Content-Type: application/json');

        try {
            $this->log("Handling AJAX request: {$action}");

            switch ($action) {
                case 'quick_toggle':
                    return $this->handleQuickToggle($postData);
                case 'reorder_descriptors':
                    return $this->handleReorderDescriptors($postData);
                case 'group_descriptors':
                    return $this->handleGroupDescriptors($postData);
                case 'batch_update':
                    return $this->handleBatchUpdate($postData);
                case 'batch_apply':
                    return $this->handleBatchApply($postData);
                case 'auto_generate_upsells':
                    return $this->handleAutoGenerateUpsells($postData);
                case 'smart_carousel_off':
                    return $this->handleSmartCarouselOff($postData);
                case 'delete_descriptor':
                    return $this->handleDeleteDescriptor($postData);
                case 'duplicate_descriptor':
                    return $this->handleDuplicateDescriptor($postData);
                case 'export_descriptors':
                    return $this->handleExportDescriptors($postData);
                case 'get_descriptor':
                    return $this->handleGetDescriptor($postData);
                default:
                    throw new Exception("Unknown AJAX action: {$action}");
            }
        } catch (Exception $e) {
            $this->log("AJAX request error: " . $e->getMessage());
            return [
                'success' => false,
                'error' => $e->getMessage(),
                'action' => $action,
            ];
        }
    }

    private function log(string $message): void
    {
        if ($this->debug) {
            error_log("[AjaxHandler] " . $message);
        }
    }

    private function handleQuickToggle(array $postData): array
    {
        $descriptorId = $postData['descriptor_id'] ?? '';
        $field = $postData['field'] ?? '';
        $value = isset($postData['value']) && $postData['value'] === 'true';

        if ($descriptorId === '' || $field === '') {
            throw new Exception("Missing required parameters: descriptor_id and field");
        }

        $allowedFields = ['enabled', 'hidden', 'useForCarousel'];
        if (!in_array($field, $allowedFields, true)) {
            throw new Exception("Invalid field: {$field}");
        }

        $this->log("Quick toggle: {$field} = " . ($value ? 'true' : 'false') . " for descriptor {$descriptorId}");

        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200
            || !isset($descriptorsResult['data']['data'])
            || !is_array($descriptorsResult['data']['data'])
        ) {
            throw new Exception("Failed to load descriptors");
        }

        $descriptor = null;
        foreach ($descriptorsResult['data']['data'] as $desc) {
            if (!empty($desc['_id']) && $desc['_id'] === $descriptorId) {
                $descriptor = $desc;
                break;
            }
        }

        if ($descriptor === null) {
            throw new Exception("Descriptor not found: {$descriptorId}");
        }

        $descriptor[$field] = $value;
        $result = $this->api->saveDescriptor($descriptor, $this->location);

        if ($result['status'] !== 200) {
            throw new Exception("Failed to save descriptor");
        }

        return [
            'success' => true,
            'message' => 'Field updated successfully',
            'field' => $field,
            'value' => $value,
            'descriptor_id' => $descriptorId,
        ];
    }

    private function handleReorderDescriptors(array $postData): array
    {
        $raw = $postData['ordered_ids'] ?? '[]';
        $ids = json_decode($raw, true);
        if (json_last_error() !== JSON_ERROR_NONE || !is_array($ids) || !$ids) {
            throw new Exception("Invalid JSON for ordered_ids");
        }

        $this->log("Reordering " . count($ids) . " descriptors");

        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200
            || !isset($descriptorsResult['data']['data'])
            || !is_array($descriptorsResult['data']['data'])
        ) {
            throw new Exception("Failed to load descriptors for reordering");
        }

        $map = [];
        foreach ($descriptorsResult['data']['data'] as $desc) {
            if (!empty($desc['_id'])) {
                $map[$desc['_id']] = $desc;
            }
        }

        $updated = [];
        $updateCount = 0;
        foreach ($ids as $i => $id) {
            if (isset($map[$id])) {
                $newPos = $i + 1;
                if (($map[$id]['ordinalPosition'] ?? 0) !== $newPos) {
                    $map[$id]['ordinalPosition'] = $newPos;
                    $updated[] = $map[$id];
                    $updateCount++;
                }
            }
        }

        if ($updateCount === 0) {
            return [
                'success' => true,
                'message' => 'No changes detected',
                'updated_count' => 0,
            ];
        }

        $errors = [];
        $success = 0;
        foreach ($updated as $desc) {
            $res = $this->api->saveDescriptor($desc, $this->location);
            if ($res['status'] === 200) {
                $success++;
            } else {
                $errors[] = $desc['name'] ?? 'unknown';
            }
        }

        if ($success === 0) {
            throw new Exception("Failed to update any descriptors");
        }

        return [
            'success' => true,
            'message' => "Updated {$success} of {$updateCount}",
            'updated_count' => $success,
            'errors' => $errors,
        ];
    }

    /**
     * Enhanced batch update to properly handle delete operations
     */
    private function handleBatchUpdate(array $postData): array
    {
        $operation = $postData['operation'] ?? '';
        $selectedIds = json_decode($postData['selected_ids'] ?? '[]', true);

        if (empty($operation)) {
            throw new Exception("Operation is required for batch update");
        }

        if (json_last_error() !== JSON_ERROR_NONE || !is_array($selectedIds) || empty($selectedIds)) {
            throw new Exception("No descriptors selected for batch update");
        }

        $this->log("Batch {$operation} for " . count($selectedIds) . " descriptors");

        // Get all descriptors
        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200) {
            throw new Exception("Failed to load descriptors for batch update");
        }

        $allDescriptors = $descriptorsResult['data']['data'] ?? $descriptorsResult['data'] ?? [];
        $selectedDescriptors = array_filter($allDescriptors, function ($desc) use ($selectedIds) {
            return in_array($desc['_id'], $selectedIds);
        });

        if (empty($selectedDescriptors)) {
            throw new Exception("No matching descriptors found for batch update");
        }

        $this->log("Found " . count($selectedDescriptors) . " descriptors to process");

        // Handle delete operation differently
        if ($operation === 'delete') {
            return $this->executeBatchDelete($selectedDescriptors);
        }

        // Handle other operations (enable, disable, show, hide)
        return $this->executeBatchToggle($selectedDescriptors, $operation);
    }

    /**
     * Handle deletion of a single descriptor
     */
    private function handleDeleteDescriptor(array $postData): array
    {
        $descriptorId = $postData['descriptor_id'] ?? '';

        if (empty($descriptorId)) {
            throw new Exception("Descriptor ID is required for deletion");
        }

        $this->log("Deleting descriptor: {$descriptorId}");

        // Get the descriptor to delete
        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200) {
            throw new Exception("Failed to load descriptors for deletion");
        }

        $allDescriptors = $descriptorsResult['data']['data'] ?? $descriptorsResult['data'] ?? [];
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

        $descriptorName = $descriptor['name'] ?? 'Unknown';
        $this->log("Found descriptor to delete: {$descriptorName}");

        // Delete the descriptor using the API
        $result = $this->api->deleteDescriptor($descriptor, $this->location);

        if ($result['status'] === 200) {
            $this->log("Successfully deleted descriptor: {$descriptorName}");
            return [
                'success' => true,
                'message' => "Descriptor '{$descriptorName}' deleted successfully",
                'descriptor_id' => $descriptorId,
                'descriptor_name' => $descriptorName
            ];
        } else {
            $error = $result['data']['error'] ?? $result['data']['message'] ?? 'Unknown error';
            throw new Exception("Failed to delete descriptor: {$error}");
        }
    }

    /**
     * Handle duplication of a descriptor
     */
    /**
     * Handle duplication of a descriptor - FIXED VERSION
     */
    private function handleDuplicateDescriptor(array $postData): array
    {
        $descriptorId = $postData['descriptor_id'] ?? '';

        if (empty($descriptorId)) {
            throw new Exception("Descriptor ID is required for duplication");
        }

        $this->log("Duplicating descriptor: {$descriptorId}");

        // Get the descriptor to duplicate
        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200) {
            throw new Exception("Failed to load descriptors for duplication");
        }

        $allDescriptors = $descriptorsResult['data']['data'] ?? $descriptorsResult['data'] ?? [];
        $originalDescriptor = null;

        foreach ($allDescriptors as $desc) {
            if ($desc['_id'] === $descriptorId) {
                $originalDescriptor = $desc;
                break;
            }
        }

        if (!$originalDescriptor) {
            throw new Exception("Descriptor not found: {$descriptorId}");
        }

        $originalName = $originalDescriptor['name'] ?? 'Unknown';
        $this->log("Found descriptor to duplicate: {$originalName}");

        // Create a copy of the descriptor
        $newDescriptor = $originalDescriptor;

        // CRITICAL: Remove the ID so a new one will be generated
        unset($newDescriptor['_id']);

        // IMPROVED: Smart name generation to avoid "(Copy) (Copy)"
        $newName = $this->generateUniqueCopyName($originalName, $allDescriptors);
        $newDescriptor['name'] = $newName;

        // Set the position to be at the end + 1
        $maxPosition = 0;
        foreach ($allDescriptors as $desc) {
            $position = intval($desc['ordinalPosition'] ?? 0);
            if ($position > $maxPosition) {
                $maxPosition = $position;
            }
        }
        $newDescriptor['ordinalPosition'] = $maxPosition + 1;

        // Ensure required fields are set
        $newDescriptor['sLocationCode'] = $this->location;
        if (empty($newDescriptor['sCorpCode'])) {
            $newDescriptor['sCorpCode'] = 'CNCK';
        }

        // IMPORTANT: Clear any potentially problematic fields that might cause conflicts
        unset($newDescriptor['createdAt']);
        unset($newDescriptor['updatedAt']);
        unset($newDescriptor['__v']);

        $this->log("Creating duplicate with name: {$newDescriptor['name']} at position {$newDescriptor['ordinalPosition']}");

        // Save the new descriptor
        $result = $this->api->saveDescriptor($newDescriptor, $this->location);

        if ($result['status'] === 200) {
            $this->log("Successfully duplicated descriptor: {$originalName} -> {$newDescriptor['name']}");

            // Get the new descriptor ID from the response if available
            $newDescriptorId = $result['data']['_id'] ?? $result['data']['id'] ?? 'unknown';

            return [
                'success' => true,
                'message' => "Descriptor duplicated successfully as '{$newDescriptor['name']}'",
                'original_id' => $descriptorId,
                'original_name' => $originalName,
                'new_id' => $newDescriptorId,
                'new_name' => $newDescriptor['name'],
                'new_position' => $newDescriptor['ordinalPosition']
            ];
        } else {
            $error = $result['data']['error'] ?? $result['data']['message'] ?? 'Unknown error';
            $this->log("Failed to save duplicate: " . $error);
            throw new Exception("Failed to duplicate descriptor: {$error}");
        }
    }

    /**
     * Generate a unique copy name that avoids duplicate "(Copy)" suffixes
     */
    private function generateUniqueCopyName(string $originalName, array $allDescriptors): string
    {
        // Get all existing names for comparison
        $existingNames = array_map(function ($desc) {
            return strtolower($desc['name'] ?? '');
        }, $allDescriptors);

        // Remove existing " (Copy)" or " (Copy X)" from the original name to get the base
        $baseName = preg_replace('/\s+\(Copy(?:\s+\d+)?\)$/i', '', $originalName);

        // Start with " (Copy)"
        $newName = $baseName . ' (Copy)';
        $counter = 1;

        // If that name exists, try " (Copy 2)", " (Copy 3)", etc.
        while (in_array(strtolower($newName), $existingNames)) {
            $counter++;
            $newName = $baseName . ' (Copy ' . $counter . ')';

            // Safety check to prevent infinite loop
            if ($counter > 100) {
                $newName = $baseName . ' (Copy ' . uniqid() . ')';
                break;
            }
        }

        return $newName;
    }

    /**
     * Execute batch delete operation
     */
    private function executeBatchDelete(array $descriptors): array
    {
        $successCount = 0;
        $errors = [];
        $deletedNames = [];

        foreach ($descriptors as $descriptor) {
            try {
                $descriptorName = $descriptor['name'] ?? 'Unknown';

                $result = $this->api->deleteDescriptor($descriptor, $this->location);

                if ($result['status'] === 200) {
                    $successCount++;
                    $deletedNames[] = $descriptorName;
                    $this->log("Successfully deleted: {$descriptorName}");
                } else {
                    $error = $result['data']['error'] ?? $result['data']['message'] ?? 'Unknown error';
                    $errors[] = "{$descriptorName}: {$error}";
                    $this->log("Failed to delete {$descriptorName}: {$error}");
                }
            } catch (Exception $e) {
                $descriptorName = $descriptor['name'] ?? 'Unknown';
                $errors[] = "{$descriptorName}: " . $e->getMessage();
                $this->log("Exception deleting {$descriptorName}: " . $e->getMessage());
            }
        }

        $totalAttempted = count($descriptors);

        if ($successCount === 0) {
            throw new Exception("Failed to delete any descriptors: " . implode('; ', $errors));
        }

        $message = "Successfully deleted {$successCount} of {$totalAttempted} descriptors";
        if (!empty($deletedNames)) {
            $message .= ": " . implode(', ', array_slice($deletedNames, 0, 3));
            if (count($deletedNames) > 3) {
                $message .= " and " . (count($deletedNames) - 3) . " more";
            }
        }

        return [
            'success' => true,
            'message' => $message,
            'operation' => 'delete',
            'affected_count' => $successCount,
            'total_attempted' => $totalAttempted,
            'deleted_names' => $deletedNames,
            'errors' => $errors
        ];
    }

    /**
     * Execute batch toggle operations (enable, disable, show, hide)
     */
    private function executeBatchToggle(array $descriptors, string $operation): array
    {
        $successCount = 0;
        $errors = [];
        $updatedNames = [];

        foreach ($descriptors as $descriptor) {
            try {
                $descriptorName = $descriptor['name'] ?? 'Unknown';

                // Apply the operation
                switch ($operation) {
                    case 'enable':
                        $descriptor['enabled'] = true;
                        break;
                    case 'disable':
                        $descriptor['enabled'] = false;
                        break;
                    case 'show':
                        $descriptor['hidden'] = false;
                        break;
                    case 'hide':
                        $descriptor['hidden'] = true;
                        break;
                    default:
                        throw new Exception("Unknown operation: {$operation}");
                }

                $result = $this->api->saveDescriptor($descriptor, $this->location);

                if ($result['status'] === 200) {
                    $successCount++;
                    $updatedNames[] = $descriptorName;
                    $this->log("Successfully {$operation}d: {$descriptorName}");
                } else {
                    $error = $result['data']['error'] ?? $result['data']['message'] ?? 'Unknown error';
                    $errors[] = "{$descriptorName}: {$error}";
                    $this->log("Failed to {$operation} {$descriptorName}: {$error}");
                }
            } catch (Exception $e) {
                $descriptorName = $descriptor['name'] ?? 'Unknown';
                $errors[] = "{$descriptorName}: " . $e->getMessage();
                $this->log("Exception during {$operation} of {$descriptorName}: " . $e->getMessage());
            }
        }

        $totalAttempted = count($descriptors);

        if ($successCount === 0) {
            throw new Exception("Failed to {$operation} any descriptors: " . implode('; ', $errors));
        }

        $message = "Successfully {$operation}d {$successCount} of {$totalAttempted} descriptors";

        return [
            'success' => true,
            'message' => $message,
            'operation' => $operation,
            'affected_count' => $successCount,
            'total_attempted' => $totalAttempted,
            'updated_names' => $updatedNames,
            'errors' => $errors
        ];
    }

    /**
     * Applies a given field/value to a batch of descriptors,
     * then optionally turns off the carousel if deals or insurance were updated.
     */
    /**
     * FIXED: Applies insurance or deals to a batch of descriptors
     */
    public function handleBatchApply(array $postData): array
    {
        $descriptorIds = json_decode($postData['descriptor_ids'] ?? '[]', true);
        $updateData = json_decode($postData['update_data'] ?? '{}', true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new Exception("Invalid JSON data provided");
        }

        if (empty($descriptorIds) || !is_array($descriptorIds)) {
            throw new Exception("No descriptors selected for batch apply");
        }

        if (empty($updateData['field'])) {
            throw new Exception("No field specified for batch apply");
        }

        $field = $updateData['field'];
        $value = $updateData['value'] ?? null;
        $mode = $updateData['mode'] ?? 'replace'; // 'replace' or 'add' for deals

        $this->log("Batch apply: {$field} to " . count($descriptorIds) . " descriptors (mode: {$mode})");

        // Load all descriptors
        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200) {
            throw new Exception("Failed to load descriptors for batch apply");
        }

        $allDescriptors = $descriptorsResult['data']['data'] ?? $descriptorsResult['data'] ?? [];

        // Create lookup map for quick access
        $descriptorMap = [];
        foreach ($allDescriptors as $desc) {
            if (!empty($desc['_id'])) {
                $descriptorMap[$desc['_id']] = $desc;
            }
        }

        $successCount = 0;
        $errors = [];
        $updatedNames = [];

        foreach ($descriptorIds as $descriptorId) {
            if (!isset($descriptorMap[$descriptorId])) {
                $errors[] = "Descriptor not found: {$descriptorId}";
                continue;
            }

            $descriptor = $descriptorMap[$descriptorId];
            $descriptorName = $descriptor['name'] ?? 'Unknown';

            try {
                // Apply the field update based on type
                $updated = false;

                switch ($field) {
                    case 'defaultInsuranceCoverage':
                        $oldValue = $descriptor['defaultInsuranceCoverage'] ?? null;
                        $descriptor['defaultInsuranceCoverage'] = $value; // Can be null to remove insurance
                        $updated = ($oldValue !== $value);
                        $this->log("Insurance update for {$descriptorName}: '{$oldValue}' -> '{$value}'");
                        break;

                    case 'deals':
                        $oldDeals = $descriptor['deals'] ?? [];
                        $newDeals = is_array($value) ? $value : [];

                        if ($mode === 'add') {
                            // Add new deals to existing ones (avoid duplicates)
                            $combinedDeals = array_unique(array_merge($oldDeals, $newDeals));
                            $descriptor['deals'] = array_values($combinedDeals);
                        } else {
                            // Replace existing deals
                            $descriptor['deals'] = $newDeals;
                        }

                        $updated = ($oldDeals !== $descriptor['deals']);
                        $this->log("Deals update for {$descriptorName}: " . count($oldDeals) . " -> " . count($descriptor['deals']) . " deals ({$mode} mode)");
                        break;

                    default:
                        throw new Exception("Unsupported field for batch apply: {$field}");
                }

                // Only save if there was actually a change
                if ($updated) {
                    $result = $this->api->saveDescriptor($descriptor, $this->location);

                    if ($result['status'] === 200) {
                        $successCount++;
                        $updatedNames[] = $descriptorName;
                        $this->log("Successfully updated {$field} for: {$descriptorName}");
                    } else {
                        $error = $result['data']['error'] ?? $result['data']['message'] ?? 'Unknown error';
                        $errors[] = "{$descriptorName}: {$error}";
                        $this->log("Failed to update {$descriptorName}: {$error}");
                    }
                } else {
                    $this->log("No changes needed for {$descriptorName} (value already set)");
                    // Count as success since the desired state is already achieved
                    $successCount++;
                    $updatedNames[] = $descriptorName . " (no change needed)";
                }

            } catch (Exception $e) {
                $errors[] = "{$descriptorName}: " . $e->getMessage();
                $this->log("Exception updating {$descriptorName}: " . $e->getMessage());
            }
        }

        // Optionally turn off carousel if deals or insurance were updated
        $carouselOffCount = 0;
        $carouselErrors = [];
        if (in_array($field, ['deals', 'defaultInsuranceCoverage'], true) && $successCount > 0) {
            try {
                $carouselResult = $this->handleSmartCarouselOff([]);
                $carouselOffCount = $carouselResult['updated_count'] ?? 0;
                $carouselErrors = $carouselResult['errors'] ?? [];
                $this->log("Smart carousel update: {$carouselOffCount} descriptors updated");
            } catch (Exception $e) {
                $this->log("Smart carousel update failed: " . $e->getMessage());
                $carouselErrors[] = "Carousel update failed: " . $e->getMessage();
            }
        }

        $totalAttempted = count($descriptorIds);

        if ($successCount === 0) {
            throw new Exception("Failed to update any descriptors. Errors: " . implode('; ', array_slice($errors, 0, 3)));
        }

        // Build success message
        $fieldDisplayName = $field === 'defaultInsuranceCoverage' ? 'insurance' : $field;
        $message = "Successfully updated {$fieldDisplayName} on {$successCount} of {$totalAttempted} descriptors";

        if ($carouselOffCount > 0) {
            $message .= " and optimized carousel settings for {$carouselOffCount} descriptors";
        }

        return [
            'success' => true,
            'message' => $message,
            'field' => $field,
            'mode' => $mode,
            'updated_count' => $successCount,
            'total_attempted' => $totalAttempted,
            'updated_names' => $updatedNames,
            'carousel_off_count' => $carouselOffCount,
            'errors' => array_merge($errors, $carouselErrors)
        ];
    }

    private function handleAutoGenerateUpsells(array $postData): array
    {
        $rawIds = $postData['descriptor_ids'] ?? '[]';
        $rawRules = $postData['rules'] ?? '{}';
        $ids = json_decode($rawIds, true);
        $rules = json_decode($rawRules, true);

        if (json_last_error() !== JSON_ERROR_NONE || !is_array($ids) || empty($ids)) {
            throw new Exception("No descriptors selected for upsell generation");
        }

        $this->log("Auto upsells: " . count($ids));

        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200
            || !isset($descriptorsResult['data']['data'])
            || !is_array($descriptorsResult['data']['data'])
        ) {
            throw new Exception("Failed to load descriptors");
        }

        $all = $descriptorsResult['data']['data'];
        $map = [];
        foreach ($all as $d) {
            if (!empty($d['_id'])) {
                $map[$d['_id']] = $d;
            }
        }

        $success = 0;
        $totalUpsells = 0;
        $errors = [];
        foreach ($ids as $id) {
            if (!isset($map[$id])) {
                continue;
            }
            $desc = $map[$id];
            try {
                $upsells = $this->generateUpsellsForDescriptor($desc, $all, $rules);
                if (!empty($upsells)) {
                    $desc['upgradesTo'] = $upsells;
                    $res = $this->api->saveDescriptor($desc, $this->location);
                    if ($res['status'] === 200) {
                        $success++;
                        $totalUpsells += count($upsells);
                    } else {
                        $errors[] = $desc['name'] ?? 'unknown';
                    }
                }
            } catch (Exception $e) {
                $errors[] = $desc['name'] . ': ' . $e->getMessage();
            }
        }

        return [
            'success' => $success > 0,
            'message' => $success > 0
                ? "Generated {$totalUpsells} upsells for {$success} items"
                : "No upsells generated",
            'updated_count' => $success,
            'total_upsells' => $totalUpsells,
            'errors' => $errors,
        ];
    }

    private function generateUpsellsForDescriptor(array $descriptor, array $allDescriptors, array $rules): array
    {
        $upsells = [];
        $name = strtolower($descriptor['name'] ?? '');

        // Rule 1: Regular → Premium same size
        if (!empty($rules['sameSize']) && strpos($name, 'regular') !== false) {
            $size = $this->extractSizeNumber($name);
            if ($size) {
                foreach ($allDescriptors as $cand) {
                    if ($cand['_id'] === $descriptor['_id']) {
                        continue;
                    }
                    $candName = strtolower($cand['name'] ?? '');
                    if (strpos($candName, 'premium') !== false
                        && $this->extractSizeNumber($candName) === $size
                    ) {
                        $upsells[] = [
                            '_id' => $cand['_id'],
                            'upgradeReason' => 'Upgrade to premium ' . ucfirst($size),
                        ];
                    }
                }
            }
        }

        // Rule 2: From any to the next size up
        if (!empty($rules['nextSize'])) {
            $currNum = $this->extractSizeNumber($name);
            if ($currNum) {
                foreach ($allDescriptors as $cand) {
                    if ($cand['_id'] === $descriptor['_id']) {
                        continue;
                    }
                    $candNum = $this->extractSizeNumber(strtolower($cand['name'] ?? ''));
                    if ($candNum === $currNum + 1) {
                        $upsells[] = [
                            '_id' => $cand['_id'],
                            'upgradeReason' => 'Step up size to ' . ucfirst($candNum),
                        ];
                    }
                }
            }
        }

        // Dedupe & limit to 3 suggestions
        $seen = [];
        $unique = [];
        foreach ($upsells as $u) {
            if (!in_array($u['_id'], $seen, true)) {
                $unique[] = $u;
                $seen[] = $u['_id'];
            }
        }

        return array_slice($unique, 0, 3);
    }

    private function handleSmartCarouselOff(array $postData): array
    {
        $this->log("Smart carousel management - starting");

        // Use DataLoader to get descriptors with inventory calculated
        require_once 'DataLoader.php';
        $dataLoader = new DataLoader($this->api, $this->location, $this->debug);

        try {
            // Load all data including inventory calculations
            $allData = $dataLoader->loadAllData();
            $allDescriptors = $allData['descriptors'] ?? [];

            $this->log("Total descriptors with inventory: " . count($allDescriptors));

            // Find descriptors to update
            $toTurnOff = [];  // 100% occupied with carousel ON
            $toTurnOn = [];   // <100% occupied with carousel OFF

            foreach ($allDescriptors as $descriptor) {
                $occupancy = $descriptor['inventory']['occupancy'] ?? 0;
                $carouselEnabled = $descriptor['useForCarousel'] ?? false;

                $this->log("Checking: " . ($descriptor['name'] ?? 'Unknown') .
                    " - Occupancy: {$occupancy}%, Carousel: " . ($carouselEnabled ? 'Yes' : 'No'));

                // Turn OFF carousel for 100% occupied units
                if ($occupancy >= 100 && $carouselEnabled) {
                    $toTurnOff[] = $descriptor;
                    $this->log("Will turn OFF carousel for: " . ($descriptor['name'] ?? 'Unknown'));
                } // Turn ON carousel for <100% occupied units
                elseif ($occupancy < 100 && !$carouselEnabled) {
                    $toTurnOn[] = $descriptor;
                    $this->log("Will turn ON carousel for: " . ($descriptor['name'] ?? 'Unknown'));
                }
            }

            $this->log("Found " . count($toTurnOff) . " to turn OFF, " . count($toTurnOn) . " to turn ON");

            if (empty($toTurnOff) && empty($toTurnOn)) {
                return [
                    'success' => true,
                    'message' => 'All descriptors already have optimal carousel settings',
                    'turned_off' => 0,
                    'turned_on' => 0,
                    'updated_ids' => []
                ];
            }

            // Process updates
            $turnedOff = 0;
            $turnedOn = 0;
            $errors = [];
            $updatedIds = [];
            $updatedDetails = [];

            // Turn OFF carousel for fully occupied units
            foreach ($toTurnOff as $descriptor) {
                $descriptor['useForCarousel'] = false;

                $res = $this->api->saveDescriptor($descriptor, $this->location);
                if ($res['status'] === 200) {
                    $turnedOff++;
                    $updatedIds[] = $descriptor['_id'];
                    $updatedDetails[] = [
                        'id' => $descriptor['_id'],
                        'name' => $descriptor['name'] ?? 'Unknown',
                        'action' => 'turned_off',
                        'occupancy' => $descriptor['inventory']['occupancy'] ?? 0
                    ];
                    $this->log("Successfully turned OFF carousel for: " . ($descriptor['name'] ?? 'Unknown'));
                } else {
                    $errors[] = ($descriptor['name'] ?? 'unknown') . ' (turn off failed)';
                }
            }

            // Turn ON carousel for available units
            foreach ($toTurnOn as $descriptor) {
                $descriptor['useForCarousel'] = true;

                $res = $this->api->saveDescriptor($descriptor, $this->location);
                if ($res['status'] === 200) {
                    $turnedOn++;
                    $updatedIds[] = $descriptor['_id'];
                    $updatedDetails[] = [
                        'id' => $descriptor['_id'],
                        'name' => $descriptor['name'] ?? 'Unknown',
                        'action' => 'turned_on',
                        'occupancy' => $descriptor['inventory']['occupancy'] ?? 0
                    ];
                    $this->log("Successfully turned ON carousel for: " . ($descriptor['name'] ?? 'Unknown'));
                } else {
                    $errors[] = ($descriptor['name'] ?? 'unknown') . ' (turn on failed)';
                }
            }

            // Build comprehensive message
            $messageParts = [];
            if ($turnedOff > 0) {
                $messageParts[] = "Turned OFF carousel for {$turnedOff} fully occupied units";
            }
            if ($turnedOn > 0) {
                $messageParts[] = "Turned ON carousel for {$turnedOn} available units";
            }

            $message = !empty($messageParts)
                ? implode(' and ', $messageParts)
                : "No changes made";

            return [
                'success' => ($turnedOff + $turnedOn) > 0,
                'message' => $message,
                'turned_off' => $turnedOff,
                'turned_on' => $turnedOn,
                'updated_count' => $turnedOff + $turnedOn,
                'updated_ids' => $updatedIds,
                'updated_details' => $updatedDetails,
                'errors' => $errors
            ];

        } catch (Exception $e) {
            $this->log("Error in smart carousel management: " . $e->getMessage());
            throw $e;
        }
    }

    private function handleGroupDescriptors(array $postData): array
    {
        $rawIds = $postData['descriptor_ids'] ?? '[]';
        $groupName = trim($postData['group_name'] ?? '');
        $ids = json_decode($rawIds, true);

        if (json_last_error() !== JSON_ERROR_NONE || !is_array($ids) || empty($ids)) {
            throw new Exception("No descriptors selected");
        }
        if ($groupName === '') {
            throw new Exception("Group name is required");
        }

        // Placeholder for future grouping logic
        return [
            'success' => true,
            'message' => 'Grouping planned for future release',
            'group_name' => $groupName,
            'descriptor_count' => count($ids),
        ];
    }

    private function handleExportDescriptors(array $postData): array
    {
        $rawIds = $postData['descriptor_ids'] ?? '[]';
        $format = $postData['format'] ?? 'json';
        $ids = json_decode($rawIds, true);

        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200
            || !isset($descriptorsResult['data']['data'])
            || !is_array($descriptorsResult['data']['data'])
        ) {
            throw new Exception("Failed to load descriptors");
        }

        $all = $descriptorsResult['data']['data'];
        if (json_last_error() === JSON_ERROR_NONE && is_array($ids) && !empty($ids)) {
            $all = array_filter($all, fn($d) => in_array($d['_id'], $ids, true));
        }

        if ($format === 'csv') {
            $csv = $this->convertToCSV($all);
            $filename = 'descriptors_' . date('Y-m-d_His') . '.csv';
            return [
                'success' => true,
                'data' => $csv,
                'format' => 'csv',
                'filename' => $filename,
            ];
        }

        return [
            'success' => true,
            'data' => array_values($all),
            'format' => 'json',
            'filename' => 'descriptors_' . date('Y-m-d_His') . '.json',
        ];
    }

    private function handleGetDescriptor(array $postData): array
    {
        $id = $postData['descriptor_id'] ?? '';
        if ($id === '') {
            throw new Exception("Descriptor ID is required");
        }

        $descriptorsResult = $this->api->getDescriptors($this->location);
        if ($descriptorsResult['status'] !== 200
            || !isset($descriptorsResult['data']['data'])
            || !is_array($descriptorsResult['data']['data'])
        ) {
            throw new Exception("Failed to load descriptors");
        }

        foreach ($descriptorsResult['data']['data'] as $d) {
            if ($d['_id'] === $id) {
                return ['success' => true, 'data' => $d];
            }
        }

        throw new Exception("Descriptor not found");
    }

    private function convertToCSV(array $descriptors): string
    {
        $out = [];
        $headers = [
            'ID', 'Name', 'Description', 'Special Text', 'Position',
            'Enabled', 'Visible', 'Carousel', 'Total Units', 'Occupancy %',
            'Availability %', 'Deals Count', 'Insurance', 'Keywords'
        ];
        $out[] = $this->arrayToCSVRow($headers);

        foreach ($descriptors as $d) {
            $row = [
                $d['_id'] ?? '',
                $d['name'] ?? '',
                $d['description'] ?? '',
                $d['specialText'] ?? '',
                $d['ordinalPosition'] ?? 0,
                !empty($d['enabled']) ? 'Yes' : 'No',
                empty($d['hidden']) ? 'Yes' : 'No',
                !empty($d['useForCarousel']) ? 'Yes' : 'No',
                $d['inventory']['total'] ?? 0,
                $d['inventory']['occupancy'] ?? 0,
                $d['inventory']['availability'] ?? 0,
                count($d['deals'] ?? []),
                !empty($d['defaultInsuranceCoverage']) ? 'Yes' : 'No',
                implode('; ', $d['criteria']['include']['keywords'] ?? []),
            ];
            $out[] = $this->arrayToCSVRow($row);
        }

        return implode("\n", $out);
    }

    private function arrayToCSVRow(array $fields): string
    {
        return implode(',', array_map(function ($f) {
            if (strpos($f, ',') !== false
                || strpos($f, '"') !== false
                || strpos($f, "\n") !== false
            ) {
                return '"' . str_replace('"', '""', $f) . '"';
            }
            return $f;
        }, $fields));
    }

    private function extractSize(string $name): ?string
    {
        if (preg_match('/(\d+(?:-\d+)?)\s*sq\s*ft/i', $name, $m)) {
            return $m[1];
        }
        return null;
    }

    private function extractSizeNumber(string $name): ?int
    {
        if (preg_match('/(\d+)(?:-(\d+))?\s*sq\s*ft/i', $name, $m)) {
            return isset($m[2]) ? (int)$m[2] : (int)$m[1];
        }
        return null;
    }
}
?>