<?php
/**
 * Data Migration Script: MySQL → PostgreSQL
 *
 * This script migrates data from the old MySQL database to the new PostgreSQL database.
 * Run this script AFTER creating the PostgreSQL tables with setup_postgresql.sql
 *
 * Usage: php migrate_mysql_to_postgresql.php
 *
 * IMPORTANT: Review and test before running in production!
 */

// Error reporting
ini_set('display_errors', 1);
error_reporting(E_ALL);

echo "===========================================\n";
echo "MySQL to PostgreSQL Migration Script\n";
echo "===========================================\n\n";

// ─────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────

// MySQL Source (old database)
$mysqlConfig = [
    'host' => '104.248.149.222',
    'username' => 'admin',
    'password' => 'redbox1234',
    'database' => 'user_management',
];

// PostgreSQL Target (new database)
$pgsqlConfig = [
    'host' => 'esapbi.postgres.database.azure.com',
    'port' => '5432',
    'username' => 'esa_pbi_admin',
    'password' => 'K9wKmtRfj3zJqRU',
    'database' => 'backend',
    'sslmode' => 'require',
];

// ─────────────────────────────────────────────────────────────
// Connect to databases
// ─────────────────────────────────────────────────────────────

echo "Connecting to MySQL source...\n";
try {
    $mysql = new PDO(
        "mysql:host={$mysqlConfig['host']};dbname={$mysqlConfig['database']};charset=utf8mb4",
        $mysqlConfig['username'],
        $mysqlConfig['password'],
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
    );
    echo "✓ MySQL connected\n";
} catch (PDOException $e) {
    die("✗ MySQL connection failed: " . $e->getMessage() . "\n");
}

echo "Connecting to PostgreSQL target...\n";
try {
    $pgsql = new PDO(
        "pgsql:host={$pgsqlConfig['host']};port={$pgsqlConfig['port']};dbname={$pgsqlConfig['database']};sslmode={$pgsqlConfig['sslmode']}",
        $pgsqlConfig['username'],
        $pgsqlConfig['password'],
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
    );
    echo "✓ PostgreSQL connected\n\n";
} catch (PDOException $e) {
    die("✗ PostgreSQL connection failed: " . $e->getMessage() . "\n");
}

// ─────────────────────────────────────────────────────────────
// Migration Functions
// ─────────────────────────────────────────────────────────────

function migrateUsers($mysql, $pgsql) {
    echo "Migrating users table...\n";

    // Get users from MySQL
    $stmt = $mysql->query("SELECT id, username, email, password, role, created_at FROM users");
    $users = $stmt->fetchAll(PDO::FETCH_ASSOC);

    $count = 0;
    $errors = 0;

    foreach ($users as $user) {
        try {
            // Check if user already exists
            $check = $pgsql->prepare("SELECT id FROM users WHERE username = ? OR email = ?");
            $check->execute([$user['username'], $user['email']]);

            if ($check->fetch()) {
                echo "  - Skipping existing user: {$user['username']}\n";
                continue;
            }

            // Insert user with auth_provider = 'local' (or 'microsoft' if they have OAuth password)
            $authProvider = (strpos($user['password'], 'oauth-') !== false) ? 'microsoft' : 'local';

            $insert = $pgsql->prepare("
                INSERT INTO users (username, email, password, role, auth_provider, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ");
            $insert->execute([
                $user['username'],
                $user['email'],
                $user['password'],
                $user['role'],
                $authProvider,
                $user['created_at']
            ]);

            $count++;
        } catch (PDOException $e) {
            echo "  ✗ Error migrating user {$user['username']}: {$e->getMessage()}\n";
            $errors++;
        }
    }

    echo "  ✓ Migrated $count users ($errors errors)\n\n";
    return $count;
}

function migratePages($mysql, $pgsql) {
    echo "Migrating pages table...\n";

    // Get pages from MySQL
    $stmt = $mysql->query("SELECT id, title, slug, content, is_secure, extension, edit_restricted, created_at, updated_at FROM pages");
    $pages = $stmt->fetchAll(PDO::FETCH_ASSOC);

    $count = 0;
    $errors = 0;

    foreach ($pages as $page) {
        try {
            // Check if page already exists
            $check = $pgsql->prepare("SELECT id FROM pages WHERE slug = ?");
            $check->execute([$page['slug']]);

            if ($check->fetch()) {
                echo "  - Skipping existing page: {$page['slug']}\n";
                continue;
            }

            $insert = $pgsql->prepare("
                INSERT INTO pages (title, slug, content, is_secure, extension, edit_restricted, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ");
            $insert->execute([
                $page['title'],
                $page['slug'],
                $page['content'],
                (bool)$page['is_secure'],
                $page['extension'],
                (bool)$page['edit_restricted'],
                $page['created_at'],
                $page['updated_at']
            ]);

            $count++;
        } catch (PDOException $e) {
            echo "  ✗ Error migrating page {$page['slug']}: {$e->getMessage()}\n";
            $errors++;
        }
    }

    echo "  ✓ Migrated $count pages ($errors errors)\n\n";
    return $count;
}

function migrateSchemaMarkups($mysql, $pgsql) {
    echo "Migrating schema_markups table...\n";

    // Check if source table exists
    try {
        $stmt = $mysql->query("SELECT id, name, schema_type, schema_data, form_data, created_at, updated_at FROM schema_markups");
        $markups = $stmt->fetchAll(PDO::FETCH_ASSOC);
    } catch (PDOException $e) {
        echo "  - Source table doesn't exist or is empty, skipping\n\n";
        return 0;
    }

    $count = 0;
    $errors = 0;

    foreach ($markups as $markup) {
        try {
            $insert = $pgsql->prepare("
                INSERT INTO schema_markups (name, schema_type, schema_data, form_data, created_at, updated_at)
                VALUES (?, ?, ?::jsonb, ?::jsonb, ?, ?)
            ");
            $insert->execute([
                $markup['name'],
                $markup['schema_type'],
                $markup['schema_data'],
                $markup['form_data'],
                $markup['created_at'],
                $markup['updated_at']
            ]);

            $count++;
        } catch (PDOException $e) {
            echo "  ✗ Error migrating schema markup {$markup['name']}: {$e->getMessage()}\n";
            $errors++;
        }
    }

    echo "  ✓ Migrated $count schema markups ($errors errors)\n\n";
    return $count;
}

// ─────────────────────────────────────────────────────────────
// Run Migration
// ─────────────────────────────────────────────────────────────

echo "Starting migration...\n";
echo "───────────────────────────────────────────\n\n";

$totalUsers = migrateUsers($mysql, $pgsql);
$totalPages = migratePages($mysql, $pgsql);
$totalSchemas = migrateSchemaMarkups($mysql, $pgsql);

echo "───────────────────────────────────────────\n";
echo "Migration Complete!\n";
echo "───────────────────────────────────────────\n";
echo "Users migrated:         $totalUsers\n";
echo "Pages migrated:         $totalPages\n";
echo "Schema markups migrated: $totalSchemas\n";
echo "\n";
echo "Next steps:\n";
echo "1. Verify data in PostgreSQL\n";
echo "2. Update .env to point to PostgreSQL\n";
echo "3. Test application login\n";
echo "4. Decommission MySQL when ready\n";
