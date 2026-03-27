/**
 * SiteFilter — Reusable PBI-slicer-style site selector component.
 *
 * Usage:
 *   <div id="my-filter"></div>
 *   SiteFilter.init('my-filter', {
 *     mode: 'multi',           // 'multi' or 'single'
 *     onChange: (siteIds) => {} // callback with selected site_id[] (integers)
 *   });
 *
 * The component fetches /api/sites once and caches the result.
 * Renders a dropdown panel with country-level and site-level checkboxes,
 * a search box, and Select All / Clear All controls.
 */
window.SiteFilter = (function () {
    'use strict';

    let _sitesCache = null;
    let _instances = {};

    function _esc(str) {
        if (str == null) return '';
        return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#x27;');
    }

    function _apiHeaders() {
        const headers = { 'Content-Type': 'application/json' };
        const token = localStorage.getItem('api_token') || '';
        if (token) headers['Authorization'] = 'Bearer ' + token;
        const meta = document.querySelector('meta[name="csrf-token"]');
        if (meta) headers['X-CSRFToken'] = meta.getAttribute('content');
        return headers;
    }

    async function _fetchSites() {
        if (_sitesCache) return _sitesCache;
        const res = await fetch('/api/sites', { headers: _apiHeaders() });
        const data = await res.json();
        _sitesCache = data.sites || [];
        return _sitesCache;
    }

    function _groupByCountry(sites) {
        const grouped = {};
        sites.forEach(s => {
            const c = s.country || 'Other';
            if (!grouped[c]) grouped[c] = [];
            grouped[c].push(s);
        });
        return grouped;
    }

    function _buildHTML(id) {
        return `
            <div class="sf-container" id="${id}-container">
                <button type="button" class="sf-toggle" id="${id}-toggle">
                    <span class="sf-label" id="${id}-label">Select sites...</span>
                    <span class="sf-arrow">&#9662;</span>
                </button>
                <div class="sf-panel" id="${id}-panel" style="display:none;">
                    <div class="sf-search-row">
                        <input type="text" class="sf-search" id="${id}-search" placeholder="Search sites...">
                    </div>
                    <div class="sf-actions">
                        <button type="button" class="sf-btn" id="${id}-select-all">Select All</button>
                        <button type="button" class="sf-btn" id="${id}-clear-all">Clear All</button>
                    </div>
                    <div class="sf-list" id="${id}-list"></div>
                </div>
            </div>`;
    }

    function _renderList(inst) {
        const search = (inst.searchEl.value || '').toLowerCase();
        const grouped = _groupByCountry(inst.sites);
        const countries = Object.keys(grouped).sort();
        let html = '';

        for (const country of countries) {
            const sites = grouped[country];
            const filtered = search
                ? sites.filter(s => s.site_code.toLowerCase().includes(search) || s.name.toLowerCase().includes(search))
                : sites;
            if (filtered.length === 0) continue;

            const allChecked = filtered.every(s => inst.selected.has(s.site_id));
            const someChecked = !allChecked && filtered.some(s => inst.selected.has(s.site_id));

            html += `<div class="sf-country">
                <label class="sf-country-label">
                    <input type="checkbox" class="sf-country-cb" data-country="${_esc(country)}"
                        ${allChecked ? 'checked' : ''} ${someChecked ? 'data-indeterminate="true"' : ''}>
                    <strong>${_esc(country)}</strong> <span class="sf-country-count">(${filtered.length})</span>
                </label>
                <div class="sf-sites">`;

            for (const s of filtered) {
                const checked = inst.selected.has(s.site_id) ? 'checked' : '';
                html += `<label class="sf-site-label">
                    <input type="checkbox" class="sf-site-cb" data-site-id="${s.site_id}" ${checked}>
                    <span class="sf-site-code">${_esc(s.site_code)}</span>
                    <span class="sf-site-name">${_esc(s.name)}</span>
                </label>`;
            }

            html += '</div></div>';
        }

        if (!html) {
            html = '<div class="sf-empty">No sites match</div>';
        }

        inst.listEl.innerHTML = html;

        // Set indeterminate state (can't be set via HTML attribute)
        inst.listEl.querySelectorAll('.sf-country-cb[data-indeterminate="true"]').forEach(cb => {
            cb.indeterminate = true;
        });
    }

    function _updateLabel(inst) {
        const count = inst.selected.size;
        if (count === 0) {
            inst.labelEl.textContent = 'Select sites...';
        } else if (count === 1) {
            const sid = Array.from(inst.selected)[0];
            const site = inst.sites.find(s => s.site_id === sid);
            inst.labelEl.textContent = site ? `${site.site_code} - ${site.name}` : '1 site';
        } else if (count === inst.sites.length) {
            inst.labelEl.textContent = `All sites (${count})`;
        } else {
            inst.labelEl.textContent = `${count} sites selected`;
        }
    }

    function _fireChange(inst) {
        _updateLabel(inst);
        if (inst.onChange) {
            inst.onChange(Array.from(inst.selected));
        }
    }

    function _bindEvents(inst) {
        const container = inst.containerEl;

        // Toggle panel
        inst.toggleEl.addEventListener('click', () => {
            const panel = inst.panelEl;
            const open = panel.style.display !== 'none';
            panel.style.display = open ? 'none' : 'block';
            if (!open) inst.searchEl.focus();
        });

        // Close on outside click
        document.addEventListener('click', (e) => {
            if (!container.contains(e.target)) {
                inst.panelEl.style.display = 'none';
            }
        });

        // Search
        inst.searchEl.addEventListener('input', () => _renderList(inst));

        // Select All
        document.getElementById(inst.id + '-select-all').addEventListener('click', () => {
            const search = (inst.searchEl.value || '').toLowerCase();
            const matching = inst.sites.filter(s =>
                !search || s.site_code.toLowerCase().includes(search) || s.name.toLowerCase().includes(search)
            );
            if (inst.mode === 'single') {
                inst.selected.clear();
                if (matching.length) inst.selected.add(matching[0].site_id);
            } else {
                matching.forEach(s => inst.selected.add(s.site_id));
            }
            _renderList(inst);
            _fireChange(inst);
        });

        // Clear All
        document.getElementById(inst.id + '-clear-all').addEventListener('click', () => {
            const search = (inst.searchEl.value || '').toLowerCase();
            if (search) {
                inst.sites.forEach(s => {
                    if (s.site_code.toLowerCase().includes(search) || s.name.toLowerCase().includes(search)) {
                        inst.selected.delete(s.site_id);
                    }
                });
            } else {
                inst.selected.clear();
            }
            _renderList(inst);
            _fireChange(inst);
        });

        // Delegate checkbox clicks
        inst.listEl.addEventListener('change', (e) => {
            const target = e.target;

            if (target.classList.contains('sf-site-cb')) {
                const siteId = parseInt(target.dataset.siteId);
                if (inst.mode === 'single') {
                    inst.selected.clear();
                    if (target.checked) inst.selected.add(siteId);
                } else {
                    if (target.checked) inst.selected.add(siteId);
                    else inst.selected.delete(siteId);
                }
                _renderList(inst);
                _fireChange(inst);
            }

            if (target.classList.contains('sf-country-cb')) {
                const country = target.dataset.country;
                const search = (inst.searchEl.value || '').toLowerCase();
                const countrySites = inst.sites.filter(s => {
                    const c = s.country || 'Other';
                    if (c !== country) return false;
                    if (search && !s.site_code.toLowerCase().includes(search) && !s.name.toLowerCase().includes(search)) return false;
                    return true;
                });
                if (inst.mode === 'single' && target.checked) {
                    inst.selected.clear();
                    if (countrySites.length) inst.selected.add(countrySites[0].site_id);
                } else {
                    countrySites.forEach(s => {
                        if (target.checked) inst.selected.add(s.site_id);
                        else inst.selected.delete(s.site_id);
                    });
                }
                _renderList(inst);
                _fireChange(inst);
            }
        });
    }

    async function init(elementId, options) {
        options = options || {};
        const el = document.getElementById(elementId);
        if (!el) { console.error('SiteFilter: element not found:', elementId); return; }

        el.innerHTML = _buildHTML(elementId);

        const inst = {
            id: elementId,
            mode: options.mode || 'multi',
            selected: new Set(),
            sites: [],
            onChange: options.onChange || null,
            containerEl: document.getElementById(elementId + '-container'),
            toggleEl: document.getElementById(elementId + '-toggle'),
            labelEl: document.getElementById(elementId + '-label'),
            panelEl: document.getElementById(elementId + '-panel'),
            searchEl: document.getElementById(elementId + '-search'),
            listEl: document.getElementById(elementId + '-list'),
        };

        _instances[elementId] = inst;

        try {
            inst.sites = await _fetchSites();
            _renderList(inst);
            _bindEvents(inst);
        } catch (e) {
            console.error('SiteFilter: failed to load sites', e);
            inst.listEl.innerHTML = '<div class="sf-empty">Failed to load sites</div>';
        }

        return inst;
    }

    function getSelected(elementId) {
        const inst = _instances[elementId];
        return inst ? Array.from(inst.selected) : [];
    }

    function setSelected(elementId, siteIds) {
        const inst = _instances[elementId];
        if (!inst) return;
        inst.selected = new Set(siteIds.map(Number));
        _renderList(inst);
        _updateLabel(inst);
    }

    function getSites(elementId) {
        const inst = _instances[elementId];
        return inst ? inst.sites : [];
    }

    return { init, getSelected, setSelected, getSites };
})();
