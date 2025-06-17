
        <?php
// Remove session_start() since it's already started
// session_start(); // REMOVE THIS LINE

// Use require_once to prevent redeclaration
require_once __DIR__ . '/../../config.php';
?>
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Select WABA Mode</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <!-- Bootstrap CSS from CDN -->
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  <script>
    function toggleFields() {
      const isCopy = document.querySelector('input[name="mode"]:checked').value === 'copy';
      document.getElementById('dst-container').style.display      = isCopy ? 'block' : 'none';
      document.getElementById('conflict-container').style.display = isCopy ? 'block' : 'none';
    }
  </script>
</head>
<body class="container mt-5">
  <h2>🔧 WABA Template Manager Setup</h2>

  <form
    method="get"
    action="/waba/wabatools"
    oninput="toggleFields()"
  >
    <div class="mb-3">
      <label class="form-label">Select Mode</label><br>
      <div class="form-check form-check-inline">
        <input class="form-check-input" type="radio" name="mode" value="manage" checked>
        <label class="form-check-label">Manage</label>
      </div>
      <div class="form-check form-check-inline">
        <input class="form-check-input" type="radio" name="mode" value="copy">
        <label class="form-check-label">Copy</label>
      </div>
    </div>

    <div class="mb-3">
      <label class="form-label">Source WABA ID</label>
      <select name="src_waba_id" class="form-select" required>
        <?php foreach ($wabaIds as $waba_id): ?>
          <option value="<?= htmlspecialchars($waba_id, ENT_QUOTES) ?>">
            <?= htmlspecialchars($waba_id, ENT_QUOTES) ?>
          </option>
        <?php endforeach; ?>
      </select>
    </div>

    <div class="mb-3" id="dst-container" style="display: none;">
      <label class="form-label">Destination WABA ID</label>
      <select name="dst_waba_id" class="form-select">
        <?php foreach ($wabaIds as $waba_id): ?>
          <option value="<?= htmlspecialchars($waba_id, ENT_QUOTES) ?>">
            <?= htmlspecialchars($waba_id, ENT_QUOTES) ?>
          </option>
        <?php endforeach; ?>
      </select>
    </div>

    <div class="mb-3" id="conflict-container" style="display: none;">
      <label class="form-label">Conflict Handling</label>
      <select class="form-select" name="copy_conflict">
        <option value="auto" selected>Auto-rename (add _1)</option>
        <option value="manual">Manual (warn and skip)</option>
      </select>
    </div>

    <button type="submit" class="btn btn-primary">Continue</button>
  </form>

  <!-- Bootstrap JS bundle from CDN -->
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>      