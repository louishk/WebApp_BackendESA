<?php
// REMOVE this line: session_start();
require_once __DIR__ . '/../../config.php';

if (!isset($_SESSION['user'])) {
    header('Location: /');
    exit;
}

// Grab GET params with defaults
$mode     = $_GET['mode']           ?? 'manage';
$src      = $_GET['src_waba_id']    ?? '';
$dst      = $_GET['dst_waba_id']    ?? '';
$conflict = $_GET['copy_conflict']  ?? 'auto';
?>
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>WABA Templates</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <!-- Bootstrap CSS CDN -->
  <link
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"
    rel="stylesheet"
  >
</head>
<body class="container py-5">
  <h1>📋 WABA Templates</h1>
  <a href="/waba/modeselect" class="btn btn-secondary mb-3">← Back to Mode Select</a>
  <div id="alerts"></div>

  <form id="template-form">
    <table class="table table-striped table-bordered">
      <thead class="table-dark">
        <tr>
          <th>#</th>
          <th>Select</th>
          <th>Name</th>
          <th>Language</th>
          <th>Category</th>
          <th>Status</th>
          <th>Variables</th>
          <th>Used Variables</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody id="template-rows">
        <tr><td colspan="9" class="text-center">Loading…</td></tr>
      </tbody>
    </table>

    <div class="d-flex gap-3">
      <button type="button" id="delete-btn" class="btn btn-danger">
        🗑️ Delete Selected
      </button>
      <?php if ($mode === 'copy'): ?>
        <button type="button" id="copy-btn" class="btn btn-primary">
          📤 Copy to Destination WABA
        </button>
      <?php endif; ?>
    </div>
  </form>

  <!-- Edit Template Modal -->
  <div class="modal fade" id="editModal" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog">
      <div class="modal-content">
        <div class="modal-header">
          <h5 class="modal-title">Edit Template</h5>
          <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
        </div>
        <div class="modal-body">
          <form id="edit-form">
            <input type="hidden" id="edit-id_key" name="id_key">
            <div class="mb-3">
              <label for="edit-name" class="form-label">Name</label>
              <input type="text" class="form-control" id="edit-name" name="name">
            </div>
            <div class="mb-3">
              <label for="edit-category" class="form-label">Category</label>
              <input type="text" class="form-control" id="edit-category" name="category">
            </div>
            <div class="mb-3">
              <label for="edit-parameter_format" class="form-label">Parameter Format</label>
              <select class="form-select" id="edit-parameter_format" name="parameter_format">
                <option value="NAMED">NAMED</option>
                <option value="NUMBERED">NUMBERED</option>
              </select>
            </div>
            <div class="mb-3">
              <label for="edit-components" class="form-label">Components (JSON)</label>
              <textarea class="form-control" id="edit-components" name="components" rows="4"></textarea>
            </div>
          </form>
        </div>
        <div class="modal-footer">
          <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
          <button type="button" class="btn btn-primary" id="save-edit-btn">Save Changes</button>
        </div>
      </div>
    </div>
  </div>

  <!-- Bootstrap JS bundle -->
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
  <script>
  (function(){
    const params = new URLSearchParams({
      mode:     <?= json_encode($mode) ?>,
      src:      <?= json_encode($src) ?>,
      dst:      <?= json_encode($dst) ?>,
      conflict: <?= json_encode($conflict) ?>
    });

    const token   = <?= json_encode(RBS_API_BEARER) ?>;
    const apiBase = <?= json_encode(RBS_API_BASE . '/chattools') ?>;

    const alertsEl = document.getElementById('alerts');
    const rowsEl   = document.getElementById('template-rows');
    const delBtn   = document.getElementById('delete-btn');
    const copyBtn  = document.getElementById('copy-btn');

    let currentTemplates = [];

    function showAlert(msg, type='info') {
      const div = document.createElement('div');
      div.innerHTML = `
        <div class="alert alert-${type} alert-dismissible fade show" role="alert">
          ${msg}
          <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>`;
      alertsEl.append(div);
    }

    async function loadTemplates() {
      rowsEl.innerHTML = '<tr><td colspan="9" class="text-center">Loading…</td></tr>';
      try {
        const resp = await fetch(
          `${apiBase}/waba_view_temp?waba_src=${encodeURIComponent(params.get('src'))}`, {
            method: 'GET', mode:   'cors',
            headers: { 'Accept':'application/json', 'Authorization': `Bearer ${token}` }
          }
        );
        const obj = await resp.json();
        if (!resp.ok) throw new Error(obj.error||resp.statusText);
        currentTemplates = obj.templates;
        renderRows(obj.templates);
      } catch (e) {
        rowsEl.innerHTML = `<tr><td colspan="9" class="text-danger text-center">${e.message}</td></tr>`;
      }
    }

    function renderRows(templates) {
      if (!templates.length) {
        rowsEl.innerHTML = '<tr><td colspan="9" class="text-center">No templates found.</td></tr>';
        return;
      }
      rowsEl.innerHTML = templates.map((t,i) => `
        <tr>
          <td>${i+1}</td>
          <td><input type="checkbox" name="template" value="${t.id_key}"></td>
          <td>${t.name}</td>
          <td>${t.language}</td>
          <td>${t.category}</td>
          <td>${t.status}</td>
          <td>${t.variable_info}</td>
          <td>${t.variable_list.join(', ')}</td>
          <td>
            <button type="button" class="btn btn-sm btn-primary edit-btn" data-key="${t.id_key}">
              ✏️ Edit
            </button>
          </td>
        </tr>
      `).join('');
    }

    function getSelected() {
      return Array.from(document.querySelectorAll('input[name="template"]:checked'))
                  .map(cb => cb.value);
    }

    async function deleteSelected() {
      const sel = getSelected();
      if (!sel.length) return showAlert('No templates selected.','warning');
      try {
        const resp = await fetch(`${apiBase}/waba_del_temp`, {
          method: 'POST', mode:   'cors',
          headers: {'Content-Type':'application/json','Authorization': `Bearer ${token}`},
          body: JSON.stringify({ waba_src: params.get('src'), templates: sel })
        });
        const obj = await resp.json();
        if (!resp.ok) throw new Error(obj.error||resp.statusText);
        if (obj.deleted.length) showAlert(`${obj.deleted.length} deleted.`,'success');
        if (obj.errors.length)   showAlert(obj.errors.join('<br>'),'danger');
        loadTemplates();
      } catch (e) {
        showAlert(e.message,'danger');
      }
    }

    // Debugged copy function
    async function copySelected() {
      const sel = getSelected();
      if (!sel.length) return showAlert('No templates selected.','warning');

      const payload = {
        waba_src:    params.get('src'),
        waba_dst:    params.get('dst'),
        templates:   sel,
        rename_mode: params.get('conflict')
      };
      console.log('Copy payload:', payload);

      try {
        const resp = await fetch(`${apiBase}/waba_copy_temp`, {
          method: 'POST',
          mode: 'cors',
          headers: {
            'Content-Type':'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify(payload)
        });
        const obj = await resp.json();
        console.log('Copy response:', obj);
        if (!resp.ok) {
          const errMsg = obj.error?.error_user_msg || obj.error?.message || resp.statusText;
          throw new Error(errMsg);
        }
        if (obj.copied && obj.copied.length) showAlert(`${obj.copied.length} copied.`,'success');
        if (obj.errors && obj.errors.length) showAlert(obj.errors.join('<br>'),'danger');
        loadTemplates();
      } catch (e) {
        showAlert(e.message,'danger');
      }
    }

    delBtn.addEventListener('click', deleteSelected);
    if (copyBtn) copyBtn.addEventListener('click', copySelected);

    // Edit flow
    rowsEl.addEventListener('click', e => {
      if (!e.target.classList.contains('edit-btn')) return;
      const idKey = e.target.dataset.key;
      const tmpl  = currentTemplates.find(t=>t.id_key===idKey);
      if (!tmpl) return showAlert('Template data not found','warning');

      document.getElementById('edit-id_key').value           = idKey;
      document.getElementById('edit-name').value             = tmpl.name;
      document.getElementById('edit-category').value         = tmpl.category;
      document.getElementById('edit-parameter_format').value = tmpl.parameter_format||'NAMED';
      document.getElementById('edit-components').value       = JSON.stringify(tmpl.components||[],null,2);

      new bootstrap.Modal(document.getElementById('editModal')).show();
    });

    document.getElementById('save-edit-btn').addEventListener('click', async () => {
      const form      = document.getElementById('edit-form');
      const idKey     = form.id_key.value;
      const name      = form.name.value.trim();
      const category  = form.category.value.trim();
      const paramFmt  = form.parameter_format.value;
      let components;
      try { components = JSON.parse(form.components.value); }
      catch { return showAlert('Components JSON is invalid','danger'); }

      const payload = { waba_src: params.get('src'), id_key: idKey };
      if (name)     payload.name = name;
      if (category) payload.category = category;
      if (paramFmt) payload.parameter_format = paramFmt;
      payload.components = components;

      try {
        const resp = await fetch(`${apiBase}/waba_edit_temp`, {
          method:'POST', mode:'cors',
          headers:{'Content-Type':'application/json','Authorization': `Bearer ${token}`},
          body: JSON.stringify(payload)
        });
        const obj = await resp.json();
        if (!resp.ok) {
          const errMsg = obj.error?.message || resp.statusText;
          throw new Error(errMsg);
        }
        showAlert('Template updated successfully','success');
        bootstrap.Modal.getInstance(document.getElementById('editModal')).hide();
        loadTemplates();
      } catch (e) {
        showAlert(e.message,'danger');
      }
    });

    loadTemplates();
  })();
  </script>
</body>
</html>