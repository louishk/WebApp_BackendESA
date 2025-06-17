
        
        <?php
// merged_masswaba.php
// v0.9 milestone
// AJAX-powered contacts with custom_attributes support and DataTables UI,
// preloading contact_inboxes, caching attrKeys via APCu,
// DataTables server-side via POST to avoid long URLs

ini_set('display_errors', 0);
error_reporting(0);

require_once dirname(__DIR__,2) . '/config.php';
if (!defined('RBS_API_BASE') || !defined('RBS_API_BEARER')) {
    http_response_code(500);
    exit('Missing config');
}
if (!function_exists('apcu_fetch')) {
    die('APCu extension required');
}

// API endpoints
define('API_INBOXES', RBS_API_BASE . '/chatwoot/inboxes');
define('API_CONTACTS', RBS_API_BASE . '/chatwoot/contacts');
define('API_SEND',     RBS_API_BASE . '/chatwoot/send_template');

define('API_LABELS_TMPL', RBS_API_BASE . '/chatwoot/contacts/%d/labels');

function cw_api(string $url, array $postData = null): array {
    $ch = curl_init($url);
    $headers = ['Authorization: Bearer ' . RBS_API_BEARER];
    $opts = [CURLOPT_RETURNTRANSFER => true, CURLOPT_HTTPHEADER => $headers];
    if ($postData !== null) {
        $opts[CURLOPT_POST] = true;
        $opts[CURLOPT_POSTFIELDS] = json_encode($postData);
        $opts[CURLOPT_HTTPHEADER][] = 'Content-Type: application/json';
    }
    curl_setopt_array($ch, $opts);
    $resp = curl_exec($ch);
    curl_close($ch);
    return json_decode($resp, true) ?: [];
}

// Handle DataTables server-side via POST
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['draw'])) {
    $draw   = intval($_POST['draw']);
    $start  = intval($_POST['start'] ?? 0);
    $length = intval($_POST['length'] ?? 15);
    $page   = floor($start / $length) + 1;

    // Gather filters
    $search = trim($_POST['search']['value'] ?? '');
    $label  = trim($_POST['label'] ?? '');
    $hasMob = isset($_POST['has_mobile']);

    // Build API params
    $params = ['page' => $page, 'per_page' => $length, 'include' => 'contact_inboxes'];
    if ($search !== '') { $params['q'] = $search; }
    if ($label !== '')  { $params['labels'] = $label; }
    if ($hasMob)        { $params['has_mobile'] = 1; }

    $resp    = cw_api(API_CONTACTS . '?' . http_build_query($params));
    $payload = $resp['payload'] ?? [];
    $total   = intval($resp['meta']['count'] ?? count($payload));

    // Cache or retrieve attrKeys
    $cacheKey = 'merged_masswaba_attrKeys';
    $attrKeys = apcu_fetch($cacheKey);
    if ($attrKeys === false) {
        $k = [];
        foreach ($payload as $c) {
            foreach ($c['custom_attributes'] ?? [] as $ck => $_) {
                $k[$ck] = true;
            }
        }
        $attrKeys = array_keys($k);
        apcu_store($cacheKey, $attrKeys, 300);
    }

    // Build data rows
    $data = [];
    foreach ($payload as $c) {
        $src = '';
        foreach ($c['contact_inboxes'] ?? [] as $ci) {
            if (($ci['inbox']['channel_type'] ?? '') === 'Channel::Whatsapp') {
                $src = $ci['source_id']; break;
            }
        }
        if (!$src) { $src = ltrim($c['phone_number'] ?? '', '+'); }
        $row = ['id'=>$c['id'],'name'=>$c['name']??'','email'=>$c['email']??'','mobile'=>$c['phone_number']??'','whatsapp_source_id'=>$src];
        foreach ($attrKeys as $ck) {
            $row[$ck] = $c['custom_attributes'][$ck] ?? '';
        }
        $data[] = $row;
    }

    header('Content-Type: application/json');
    echo json_encode(['draw'=>$draw,'recordsTotal'=>$total,'recordsFiltered'=>$total,'data'=>$data,'attrKeys'=>$attrKeys], JSON_THROW_ON_ERROR);
    exit;
}

// Initial page load
$inbData = cw_api(API_INBOXES)['payload'] ?? [];
$inboxes = array_filter($inbData, fn($i)=>(($i['channel_type']??'')==='Channel::Whatsapp'));
// Preload attrKeys first page
$cacheInit = 'merged_masswaba_attrKeys_init';
$attrKeys = apcu_fetch($cacheInit);
if ($attrKeys === false) {
    $pr = cw_api(API_CONTACTS . '?page=1&per_page=15&include=contact_inboxes');
    $pp = $pr['payload'] ?? [];
    $tk = [];
    foreach ($pp as $c) { foreach ($c['custom_attributes'] ?? [] as $ck=>$_) { $tk[$ck]=true; } }
    $attrKeys = array_keys($tk);
    apcu_store($cacheInit, $attrKeys, 300);
}
?>
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Bulk WhatsApp Sender</title>
  <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
  <link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.4.1/css/buttons.dataTables.min.css">
  <link rel="stylesheet" href="https://cdn.datatables.net/colreorder/1.6.2/css/colReorder.dataTables.min.css">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <script>
    // Expose API endpoint and bearer for mw.js
    window.API_SEND_URL = "<?= API_SEND ?>";
    window.RBS_API_BEARER = "<?= RBS_API_BEARER ?>";
  </script>
  <style>
    .left-pane { position:sticky; top:0; height:100vh; overflow:auto; }
    .table-wrap { overflow-x:auto; }
    #contactsTbl th,#contactsTbl td { white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    /* Limit column visibility dropdown height */
    div.dt-button-collection {
      max-height: 300px;
      overflow-y: auto;
    }
  </style>
</head>
<body>
<div class="container-fluid"><div class="row">
  <div class="col-md-3 bg-light p-4 left-pane">
    <h5>Filters</h5>
    <form id="filterForm" class="mb-4">
      <input type="text" name="search[value]" placeholder="Search Name/Email" class="form-control mb-2">
      <input type="text" name="label" placeholder="Label" class="form-control mb-2">
      <button type="submit" class="btn btn-primary w-100">Apply Filters</button>
    </form>
    <hr>
    <h5>Inbox & Template</h5>
    <select id="inboxSelect" class="form-select mb-2"><option value="">– select inbox –</option>
      <?php foreach($inboxes as $inb):
        $tpl = htmlspecialchars(json_encode($inb['message_templates'] ?? [], JSON_UNESCAPED_UNICODE), ENT_QUOTES);
      ?>
      <option value="<?= $inb['id'] ?>" data-templates="<?= $tpl ?>"><?= htmlspecialchars($inb['name']) ?></option>
      <?php endforeach; ?>
    </select>
    <select id="templateSelect" class="form-select mb-2"><option>– select template –</option></select>
    <div id="templateComponents" class="mb-2"></div>
    <button id="blastBtn" class="btn btn-success w-100">Blast to Selected</button>
    <div id="selCount" class="mt-2">0 selected</div>
  </div>

  <div class="col-md-9 p-4">
    <h4>Chatwoot Contacts</h4>
    <div class="table-wrap">
      <table id="contactsTbl" class="display stripe hover" style="width:100%">
        <thead>
          <tr>
            <th><input type="checkbox" id="hdrChk"></th>
            <th>Name</th>
            <th>Email</th>
            <th>Mobile</th>
            <?php foreach($attrKeys as$key):
              $d = preg_replace('/^fss(?:\.contacts)?\./i','',$key);
            ?>
            <th><?=htmlspecialchars($d)?></th>
            <?php endforeach; ?>
          </tr>
        </thead>
      </table>
    </div>
  </div>
</div></div>

<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/buttons.colVis.min.js"></script>
<script src="https://cdn.datatables.net/colreorder/1.6.2/js/dataTables.colReorder.min.js"></script>
<script src="https://backend.redboxstorage.hk/pages/chatwoot/mw.js"></script>

<script>
// API endpoint & bearer (set inline above head or body)
// Custom attribute keys from server
const attrKeys = <?= json_encode($attrKeys) ?>;

$(function() {
  // Initialize DataTable
  const table = $('#contactsTbl').DataTable({
    serverSide: true,
    processing: true,
    ajax: {
      url: window.location.pathname,
      type: 'POST',
      data: function(d) {
        // build server-side parameters, including filter form values
        d.label = $('[name=label]').val();
        d.has_mobile = $('#hasMobile').prop('checked') ? 1 : null;
        // override default search with custom filter input
        d.search = d.search || {};
        d.search.value = $('#filterForm input[name="search[value]"]').val();
        return d;
      }
    },

    columns: (function() {
      const cols = [
        { data: null, defaultContent: '<input class="rowChk" type="checkbox">', orderable: false },
        { data: 'name' },
        { data: 'email' },
        { data: 'mobile' }
      ];
      attrKeys.forEach(key => {
        cols.push({ data: row => row[key] || '' , title: key.replace(/^fss(?:\.contacts)?\./i, ''), orderable: true });
      });
      return cols;
    })(),
    columnDefs: [ { targets: Array.from({ length: attrKeys.length }, (_, i) => 4 + i), visible: false } ],
    dom: 'Bfrtip', buttons: ['colvis'], colReorder: true, stateSave:true, autoWidth:false, searching:true,
    pageLength: 15, lengthMenu: [[15,30],[15,30]],
    rowCallback: (row, data) => $(row).attr('data-whatsapp-source-id', data.whatsapp_source_id)
  });

  // Master checkbox
  $('#hdrChk').on('click', function() {
    const checked = $(this).prop('checked');
    table.rows().nodes().to$().find('.rowChk').prop('checked', checked);
    updateSelCount();
  });

  // Column search inputs
  // Clone header row for per-column filtering
  $('#contactsTbl thead tr').clone().appendTo('#contactsTbl thead');
  $('#contactsTbl thead tr:eq(1) th').each(function(i) {
    if (i > 0) {
      $(this).html('<input class="col-search form-control form-control-sm" placeholder="Search '+$('#contactsTbl thead tr:eq(0) th').eq(i).text()+'"/>');
      $('input', this).on('keyup change clear', function() {
        if (table.column(i).search() !== this.value) {
          table.column(i).search(this.value).draw();
        }
      });
    } else {
      $(this).text('');
    }
  });

  // Apply filters
  $('#filterForm').on('submit', function(e) {
    e.preventDefault();
    table.ajax.reload();
  });
  $('#filterForm').on('submit', e => { e.preventDefault(); table.ajax.reload(); });

  // When inbox changes, render templates via mw.js
  $('#inboxSelect').on('change', function() {
    const templates = $(this).find(':selected').data('templates') || [];
    applyTemplates(templates);
  });

  // Blast to selected contacts
  $('#blastBtn').on('click', function() {
    const inboxId = $('#inboxSelect').val();
    const templateName = $('#templateSelect').val();
    if (!inboxId || !templateName) return alert('Select inbox & template');

    const components = {};
    $('#templateComponents').find('input,textarea').each(function() {
      components[this.name] = $(this).val();
    });

    const toSend = [];
    table.rows().every(function() {
      const rowData = this.data();
      const $row = $(this.node());
      if ($row.find('.rowChk').prop('checked')) {
        toSend.push({
          contact_id: rowData.id,
          inbox_id: inboxId,
          template_name: templateName,
          components: components
        });
      }
    });
    if (!toSend.length) return alert('No contacts selected');

    // Send via mw.js internal function
    toSend.forEach(item => postSendTemplate(item));
    alert('Blast started for ' + toSend.length + ' contacts');
  });
});
</script>
</body>
</html>
            