<?php
session_start();
require __DIR__ . '/../config.php';
require_role(['admin', 'editor']);

$baseDir   = __DIR__ . '/../pages';
$id        = isset($_GET['id']) ? (int) $_GET['id'] : null;
$editing   = $id > 0;
$errors    = [];

$data = [
    'title'           => '',
    'slug'            => '',
    'content'         => '',
    'is_secure'       => 0,
    'extension'       => 'php',
    'edit_restricted' => 0,
];
$dataDir  = '';
$dataName = '';

if ($editing) {
    $stmt = $pdo->prepare("SELECT title, slug, content, is_secure, extension, edit_restricted FROM pages WHERE id = ?");
    $stmt->execute([$id]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    if (!$row) exit('Page not found.');
    $data = array_merge($data, $row);

    // Enforce edit restriction
    if ($data['edit_restricted'] && $_SESSION['user']['role'] !== 'admin') {
        http_response_code(403);
        exit('Only admins can edit this page.');
    }

    $parts    = explode('/', $data['slug']);
    $dataName = array_pop($parts);
    $dataDir  = implode('/', $parts);
}

function getAllDirs(string $dir, string $prefix = '') {
    $out = [];
    foreach (scandir($dir) as $entry) {
        if ($entry === '.' || $entry === '..') continue;
        $full = "$dir/$entry";
        if (is_dir($full)) {
            $rel = $prefix === '' ? $entry : "$prefix/$entry";
            $out[$rel] = $rel;
            $out += getAllDirs($full, $rel);
        }
    }
    return $out;
}
$dirs = getAllDirs($baseDir);
asort($dirs);

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $data['title']           = trim($_POST['title'] ?? '');
    $data['content']         = $_POST['content'] ?? '';
    $data['is_secure']       = isset($_POST['is_secure']) ? 1 : 0;
    $data['edit_restricted'] = isset($_POST['edit_restricted']) ? 1 : 0;

    $rawDir    = trim($_POST['directory'] ?? '');
    $dirClean  = preg_replace('/[^a-z0-9\-\/]/', '', strtolower($rawDir));
    $dirClean  = trim($dirClean, '/');
    $dataDir   = $dirClean;

    $rawName   = trim($_POST['filename'] ?? '');
    $nameClean = preg_replace('/[^a-z0-9\-]/', '', strtolower($rawName));
    $dataName  = $nameClean;

    if ($nameClean === '') {
        $errors[] = 'Filename is required.';
    } else {
        $data['slug'] = $dirClean !== '' ? $dirClean . '/' . $nameClean : $nameClean;
    }

    $rawExt = strtolower(trim($_POST['extension'] ?? 'php'));
    $allowed = ['php','html','js','css','txt','env'];
    $data['extension'] = in_array($rawExt, $allowed) ? $rawExt : 'php';

    if ($data['title'] === '') {
        $errors[] = 'Title is required.';
    }
    if (!preg_match('#^[a-z0-9]+(?:/[a-z0-9\-]+)*$#', $data['slug'])) {
        $errors[] = 'Invalid directory/filename.';
    }
    if (!is_writable($baseDir)) {
        $errors[] = "Pages directory not writable.";
    }

    if (empty($errors)) {
        if ($editing) {
            $stmtOld = $pdo->prepare("SELECT slug, extension FROM pages WHERE id = ?");
            $stmtOld->execute([$id]);
            list($oldSlug, $oldExt) = $stmtOld->fetch(PDO::FETCH_NUM);
            $oldFile = "$baseDir/{$oldSlug}.{$oldExt}";
            $newFile = "$baseDir/{$data['slug']}.{$data['extension']}";
            $newDir  = dirname($newFile);
            if (!is_dir($newDir)) mkdir($newDir, 0755, true);
            if (file_exists($oldFile)) {@rename($oldFile, $newFile);}

            $sql = "UPDATE pages SET title=?, slug=?, content=?, is_secure=?, extension=?, edit_restricted=?, updated_at=NOW() WHERE id=?";
            $params = [$data['title'], $data['slug'], $data['content'], $data['is_secure'], $data['extension'], $data['edit_restricted'], $id];
        } else {
            $sql = "INSERT INTO pages (title, slug, content, is_secure, extension, edit_restricted, updated_at) VALUES (?, ?, ?, ?, ?, ?, NOW())";
            $params = [$data['title'], $data['slug'], $data['content'], $data['is_secure'], $data['extension'], $data['edit_restricted']];
        }
        $pdo->prepare($sql)->execute($params);
        if (!$editing) {$id = $pdo->lastInsertId(); $editing = true;}
    }

    if (empty($errors)) {
        $file = "$baseDir/{$data['slug']}.{$data['extension']}";
        $dir  = dirname($file);
        if (!is_dir($dir)) mkdir($dir, 0755, true);
        if (file_put_contents($file, $data['content']) === false) {
            $errors[] = "Failed to write page file: {$file}";
        }
    }

    if (empty($errors)) {
        header('Location: list_pages.php');
        exit;
    }
}
?>
<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title><?= $editing ? 'Edit' : 'Create' ?> Page</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/ace-builds@1.4.12/src-min-noconflict/ace.js"></script>
<script src="https://cdn.jsdelivr.net/npm/ace-builds@1.4.12/src-min-noconflict/mode-php.js"></script>
<script src="https://cdn.jsdelivr.net/npm/ace-builds@1.4.12/src-min-noconflict/theme-monokai.js"></script>
</head>
<body class="bg-light">
<div class="container py-4">
  <h1 class="mb-4"><?= $editing ? 'Edit' : 'Create' ?> Page</h1>
  <?php if ($errors): ?>
    <div class="alert alert-danger"><ul class="mb-0">
      <?php foreach ($errors as $e): ?>
        <li><?= htmlspecialchars($e, ENT_QUOTES) ?></li>
      <?php endforeach; ?>
    </ul></div>
  <?php endif; ?>

  <form method="post">
    <div class="mb-3">
      <label class="form-label">Title</label>
      <input name="title" class="form-control" required value="<?= htmlspecialchars($data['title'], ENT_QUOTES) ?>">
    </div>

    <div class="row g-3 mb-3">
      <div class="col-md-6">
        <label class="form-label">Directory</label>
        <select name="directory" class="form-select">
          <option value="">/ (root)</option>
          <?php foreach ($dirs as $d): ?>
            <option value="<?= htmlspecialchars($d, ENT_QUOTES) ?>" <?= $dataDir === $d ? 'selected' : '' ?>>
              <?= htmlspecialchars($d, ENT_QUOTES) ?>
            </option>
          <?php endforeach; ?>
        </select>
      </div>
      <div class="col-md-6">
        <label class="form-label">Filename</label>
        <input name="filename" class="form-control" required value="<?= htmlspecialchars($dataName, ENT_QUOTES) ?>">
        <div class="form-text">e.g. `masswaba` (no extension)</div>
      </div>
    </div>

    <div class="mb-3">
      <label class="form-label">Extension</label>
      <select name="extension" class="form-select" required>
        <?php foreach (['php','html','js','css','txt','env'] as $ext): ?>
          <option value="<?= $ext ?>" <?= $data['extension'] === $ext ? 'selected' : '' ?>>.<?= $ext ?></option>
        <?php endforeach; ?>
      </select>
    </div>

    <div class="form-check mb-3">
      <input type="checkbox" name="is_secure" id="is_secure" class="form-check-input" <?= $data['is_secure'] ? 'checked' : '' ?>>
      <label class="form-check-label" for="is_secure">🔒 Require Login to View</label>
    </div>

    <?php if ($_SESSION['user']['role'] === 'admin'): ?>
    <div class="form-check mb-3">
      <input type="checkbox" name="edit_restricted" id="edit_restricted" class="form-check-input" <?= $data['edit_restricted'] ? 'checked' : '' ?>>
      <label class="form-check-label" for="edit_restricted">🛡 Only Admins Can Edit</label>
    </div>
    <?php endif; ?>

    <div class="mb-3">
      <label class="form-label">Content</label>
      <div id="editor" style="height:600px;border:1px solid #ced4da;">
        <?= htmlspecialchars($data['content'], ENT_QUOTES) ?>
      </div>
      <textarea name="content" id="content" hidden></textarea>
    </div>

    <button type="submit" class="btn btn-primary"><?= $editing ? 'Save Changes' : 'Create Page' ?></button>
    <?php if ($editing): ?>
      <a href="/<?= htmlspecialchars($data['slug'], ENT_QUOTES) ?>" target="_blank" class="btn btn-outline-secondary ms-2">Preview</a>
    <?php endif; ?>
    <a href="list_pages.php" class="btn btn-secondary ms-2">Cancel</a>
  </form>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/js/bootstrap.bundle.min.js"></script>
<script>
  document.addEventListener('DOMContentLoaded', () => {
    const editor = ace.edit("editor");
    editor.session.setMode("ace/mode/php");
    editor.setTheme("ace/theme/monokai");
    editor.session.setUseWrapMode(true);
    editor.setOptions({ fontSize: "14px" });
    document.querySelector('form').addEventListener('submit', () => {
      document.getElementById('content').value = editor.getValue();
    });
  });
</script>
</body>
</html>