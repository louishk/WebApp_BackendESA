<?php
/**
 * Configuration Class for RapidStor Descriptor Manager
 */

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

    // Default descriptor template for new descriptors
    const DEFAULT_DESCRIPTOR = [
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
        'defaultInsuranceCoverage' => '6723aaef41549342379e4dfd',
        'sCorpCode' => 'CNCK'
    ];

    public static function getLocationName($code)
    {
        return self::LOCATIONS[$code] ?? "Unknown Location ($code)";
    }

    public static function isValidLocation($code)
    {
        return array_key_exists($code, self::LOCATIONS);
    }
}
?>