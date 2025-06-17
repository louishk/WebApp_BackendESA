
        // masswaba.js

// Expect these globals to be set before this script is loaded:
//   window.API_SEND_URL (string)
//   window.RBS_API_BEARER (string)

// Common DOM selectors
const hdr        = document.getElementById('hdrChk');
const selCountEl = document.getElementById('selCount');
const inboxSel   = document.getElementById('inboxSelect');
const tplSel     = document.getElementById('templateSelect');
const compDiv    = document.getElementById('templateComponents');
const blastBtn   = document.getElementById('blastBtn');

const API_SEND   = window.API_SEND_URL;
const API_BEARER = window.RBS_API_BEARER;

function updateSelCount() {
  selCountEl.textContent = document.querySelectorAll('.rowChk:checked').length + ' selected';
}

// Header selects all
hdr.addEventListener('change', () => {
  document.querySelectorAll('.rowChk').forEach(cb => {
    if (cb.closest('tr').style.display !== 'none') cb.checked = hdr.checked;
  });
  updateSelCount();
});

// Individual row count update
document.querySelectorAll('.rowChk').forEach(cb => cb.addEventListener('change', updateSelCount));

// Populate template dropdown when inbox changes
inboxSel.addEventListener('change', () => {
  tplSel.innerHTML = '<option value="">Select Template</option>';
  compDiv.innerHTML = '';
  const raw = inboxSel.selectedOptions[0]?.dataset.templates || '[]';
  let templates = [];
  try {
    templates = JSON.parse(raw);
  } catch (_) {
    console.error('Invalid templates JSON');
  }
  templates.forEach(t => {
    const opt = document.createElement('option');
    opt.value = t.name;
    opt.textContent = `${t.name} (${t.language})`;
    opt.dataset.language   = t.language;
    opt.dataset.category   = t.category;
    opt.dataset.components = JSON.stringify(t.components || []);
    tplSel.appendChild(opt);
  });
});

// Render components on template select
tplSel.addEventListener('change', () => {
  compDiv.innerHTML = '';
  const sel = tplSel.selectedOptions[0];
  if (!sel || !sel.value) return;
  let components = [];
  try {
    components = JSON.parse(sel.dataset.components || '[]');
  } catch (_) {
    console.error('Invalid components JSON');
  }
  components.forEach(c => {
    const el = document.createElement('div');
    el.className = 'mb-2';
    if (c.type === 'HEADER' && c.text) el.innerHTML = `<strong>HEADER:</strong> ${c.text}`;
    if (c.type === 'BODY'   && c.text) el.innerHTML = `<strong>BODY:</strong> ${c.text}`;
    if (c.type === 'FOOTER' && c.text) el.innerHTML = `<strong>FOOTER:</strong> ${c.text}`;
    if (c.type === 'BUTTONS' && Array.isArray(c.buttons)) {
      const btns = c.buttons.map(b => b.text || b.phone_number).join(', ');
      el.innerHTML = `<strong>BUTTONS:</strong> ${btns}`;
    }
    compDiv.appendChild(el);
  });
});

// Blast action
blastBtn.addEventListener('click', () => {
  const inboxId = parseInt(inboxSel.value, 10);
  const tplOpt = tplSel.selectedOptions[0];
  if (!inboxId || !tplOpt || !tplOpt.value) {
    return alert('Please select both an inbox and a template.');
  }

  // assemble content
  const comps = JSON.parse(tplOpt.dataset.components || '[]');
  const parts = [];
  ['HEADER','BODY','FOOTER'].forEach(type => {
    const cmp = comps.find(c => c.type === type);
    if (cmp?.text) parts.push(cmp.text);
  });
  const content = parts.join('\n\n');

  const payloadTemplate = {
    inbox_id: inboxId,
    message: {
      content,
      template_params: {
        name: tplOpt.value,
        category: tplOpt.dataset.category,
        language: tplOpt.dataset.language,
        processed_params: {}
      }
    }
  };

  document.querySelectorAll('.rowChk:checked').forEach(cb => {
    const row = cb.closest('tr');
    let sid = row.dataset.whatsappSourceId || row.dataset.phone || '';
    sid = sid.replace(/^\+/, '');
    const convs = JSON.parse(row.dataset.conversations || '[]');
    const openConv = convs.find(c => c.inbox_id === inboxId && c.status === 'open');

    const payload = { source_id: sid, ...payloadTemplate };
    if (openConv) payload.conversation_id = openConv.id;

    fetch(API_SEND, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${API_BEARER}`
      },
      body: JSON.stringify(payload)
    })
    .then(res => res.json().then(data => res.ok ? console.log('Sent', data) : console.error('Error', data)))
    .catch(e => console.error('Fetch error', e));
  });

  alert('Blast dispatched to ' + document.querySelectorAll('.rowChk:checked').length + ' contacts');
});
      