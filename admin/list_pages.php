<?php
session_start();
require __DIR__ . '/../config.php';
require_role(['admin', 'editor']);

$baseDir = realpath(__DIR__ . '/../pages');

if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['new_dir'])) {
  $newDirRaw = trim($_POST['new_dir']);
  $newDir    = preg_replace('/[^a-z0-9\-]/', '', strtolower($newDirRaw));
  $parent    = trim($_POST['parent_dir'] ?? '');
  $target    = $baseDir . ($parent !== '' ? "/{$parent}" : '') . "/{$newDir}";
  if ($newDir !== '' && !is_dir($target)) {
    mkdir($target, 0755, true);
  }
  header('Location: list_pages.php');
  exit;
}

$stmt = $pdo->query("SELECT id, title, slug, extension, is_secure, edit_restricted FROM pages ORDER BY slug ASC");
$flatPages = $stmt->fetchAll(PDO::FETCH_ASSOC);

function buildFSTree(string $dir, string $prefix = '') {
  $tree = [];
  foreach (scandir($dir) as $entry) {
    if ($entry === '.' || $entry === '..') continue;
    $full = "$dir/$entry";
    if (is_dir($full)) {
      $rel = $prefix === '' ? $entry : "$prefix/$entry";
      $tree[$entry] = [
        '__page'     => null,
        '__children' => buildFSTree($full, $rel),
        '__isDir'    => true,
      ];
    }
  }
  return $tree;
}
$tree = buildFSTree($baseDir);

foreach ($flatPages as $p) {
  $parts = explode('/', $p['slug']);
  $sub   = &$tree;
  foreach ($parts as $i => $seg) {
    if (!isset($sub[$seg])) {
      $sub[$seg] = ['__page'=>null, '__children'=>[], '__isDir'=>false];
    }
    if ($i === count($parts) - 1) {
      $sub[$seg]['__page'] = $p;
    }
    $sub = &$sub[$seg]['__children'];
  }
  unset($sub);
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

function renderFile(array $p) {
  $slug   = htmlspecialchars($p['slug'], ENT_QUOTES);
  $title  = htmlspecialchars($p['title'], ENT_QUOTES);
  $lock   = $p['is_secure'] ? ' 🔒' : ' 🌐';
  $shield = !empty($p['edit_restricted']) ? ' 🛡' : '';
  $canEdit = $_SESSION['user']['role'] === 'admin' || empty($p['edit_restricted']);

  echo '<div class="file-item d-flex justify-content-between align-items-center">';
  echo '  <div><span class="file-icon">📄</span> '
     .  "<strong>{$title}</strong>"
     .  "<small class=\"text-muted\"> (/{$slug}){$lock}{$shield}</small></div>";
  echo '  <div class="file-actions">';
  echo "    <a href=\"/{$slug}\" target=\"_blank\" class=\"btn btn-sm btn-info\">Preview</a> ";
  if ($canEdit) {
    echo "    <a href=\"edit_page.php?id={$p['id']}\" class=\"btn btn-sm btn-primary\">Edit</a> ";
    echo "    <a href=\"delete_page.php?id={$p['id']}\" class=\"btn btn-sm btn-danger\""
       . " onclick=\"return confirm('Delete page ". addslashes($title) ." ?')\">Delete</a>";
  } else {
    echo "    <button class=\"btn btn-sm btn-secondary\" disabled>Edit</button> ";
    echo "    <button class=\"btn btn-sm btn-secondary\" disabled>Delete</button>";
  }
  echo '  </div>';
  echo '</div>';
}

function renderTree(array $node, string $path = '') {
  echo '<ul class="tree-list">';
  foreach ($node as $name => $entry) {
    $isDir       = !empty($entry['__isDir']);
    $page        = $entry['__page'];
    $children    = $entry['__children'];
    $currentPath = $path === '' ? $name : "$path/$name";

    echo '<li>';
    if ($isDir) {
      echo '<details open>';
      echo '<summary>';
      echo '<span class="folder-icon">📁</span> '
         .  htmlspecialchars($name, ENT_QUOTES)
         .  '<a href="delete_folder.php?path='
         .  urlencode($currentPath)
         .  '" class="btn btn-sm btn-danger ms-2"'
         .  " onclick=\"return confirm('Delete entire folder "
         .  addslashes($currentPath)
         .  " and its pages?')\">Delete Folder</a>";
      echo '</summary>';
      if ($page) {
        renderFile($page);
      }
      renderTree($children, $currentPath);
      echo '</details>';
    } elseif ($page) {
      renderFile($page);
    }
    echo '</li>';
  }
  echo '</ul>';
}
?>
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Manage Pages</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    .tree-header {
      display: flex; font-weight: 500;
      padding: .5em .5em; border-bottom: 2px solid #333; margin-bottom: .5em;
    }
    .tree-header .col-name { flex: 1; }
    .tree-header .col-actions { width: auto; }
    .tree-list { list-style: none; padding-left: 0; margin: 0; }
    .tree-list ul {
      list-style: none; margin: 0;
      padding-left: 1em; border-left: 1px dashed #ccc;
    }
    .tree-list li { margin: .3em 0; }
    details > summary {
      cursor: pointer; font-size: 1.25rem; font-weight: 500;
      list-style: none; outline: none; padding-left: 1.75em; position: relative;
    }
    details > summary::before {
      content: "▶"; position: absolute; left: 0; top: .15em;
      transform-origin: center center; transition: transform .2s ease;
    }
    details[open] > summary::before {
      transform: rotate(90deg);
    }
    .folder-icon { margin-right: .5em; }
    .file-item {
      display: flex; justify-content: space-between; align-items: center;
      padding: .3em .5em; font-size: 1rem;
    }
    .file-icon { margin-right: .5em; }
    .file-actions .btn { margin-left: .3em; }
    .dir-form {
      background: #fff; padding: 1em; border: 1px solid #ddd; margin-bottom: 1.5em;
    }
  </style>
</head>
<body class="bg-light">
  <div class="container py-4">
    <h1 class="mb-4">Manage Pages</h1>
    <div class="d-flex mb-3">
      <a href="edit_page.php" class="btn btn-success">+ New Page</a>
      <button id="expand-all" class="btn btn-outline-secondary ms-2" disabled>Expand All</button>
      <button id="collapse-all" class="btn btn-outline-secondary ms-2" disabled>Collapse All</button>
      <a href="admin.php" class="btn btn-secondary ms-auto">← Back to Admin</a>
    </div>
    <div class="dir-form">
      <h5>Create Directory</h5>
      <form method="post" class="row g-2 align-items-end">
        <div class="col-md-4">
          <label for="parent_dir" class="form-label">Parent Directory</label>
          <select name="parent_dir" id="parent_dir" class="form-select">
            <option value="">/ (root)</option>
            <?php foreach ($dirs as $d): ?>
              <option value="<?= htmlspecialchars($d,ENT_QUOTES) ?>">
                <?= htmlspecialchars($d,ENT_QUOTES) ?>
              </option>
            <?php endforeach; ?>
          </select>
        </div>
        <div class="col-md-4">
          <label for="new_dir" class="form-label">Directory Name</label>
          <input type="text" name="new_dir" id="new_dir" class="form-control" required pattern="[a-z0-9\-]+">
        </div>
        <div class="col-auto">
          <button type="submit" class="btn btn-primary">Create</button>
        </div>
      </form>
    </div>
    <div class="tree-header">
      <div class="col-name">Name</div>
      <div class="col-actions">Actions</div>
    </div>
    <?php renderTree($tree); ?>
  </div>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>