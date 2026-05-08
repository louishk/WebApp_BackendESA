"""
IglooPinSyncPipeline — reconcile Igloo keypad PINs with SiteLink gate access codes.

"Should have PIN" per unit × keypad:
    is_rented                                     (always enforced, not a toggle)
    AND (NOT cfg.revoke_on_not_rentable OR bRentable)
    AND (NOT cfg.revoke_on_gate_locked  OR NOT is_gate_locked)
    AND (NOT cfg.revoke_on_overlocked   OR NOT is_overlocked)
    AND has_valid_gate_code (4-10 digits)

For every unit that has an assigned keypad:
  - should_have_pin = True, ESA PIN already matches -> skip
  - should_have_pin = True, ESA PIN differs/absent -> revoke stale + push new
  - should_have_pin = False, ESA PIN present -> revoke (block access)
  - should_have_pin = False, ESA PIN absent -> skip (silent)

PIN ownership is signalled by name STARTING WITH "ESA-{site_id}-{unit_id}".
The full name is "ESA-{site_id}-{unit_id} {unit_name}" — the unit_name is
appended for human reference (visible in the Igloo dashboard / on-keypad
listing) but is NOT used for reconciliation. Match by prefix.

Legacy manually-pushed PINs (plain unit names) are left untouched.

Missing SmartLockSiteConfig row -> all three policy flags treated as TRUE (secure default).
Missing ccws_units row -> bRentable = False (fail-safe: no PIN).
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.config import get_engine, session_scope

logger = logging.getLogger(__name__)

_PIN_RE = re.compile(r'^\d{4,10}$')
_DIGIT_REDACT_RE = re.compile(r'\b\d{4,10}\b')


def _redact_digits(s: str) -> str:
    """Redact 4-10 digit sequences (likely PINs) from error messages
    before persisting to the audit log."""
    return _DIGIT_REDACT_RE.sub('<redacted>', s or '')


def _esa_tag(site_id: int, unit_id: int, unit_name: Optional[str] = None) -> str:
    """Generate the access-entry name for an ESA-owned PIN.

    Format: "ESA-{site}-{unit} {unit_name}" — unit_name is a human-readable
    suffix shown in the Igloo dashboard. Reconciliation is by prefix match
    via _esa_tag_matches(); the unit_name suffix can change without
    breaking ownership.
    """
    base = f"ESA-{site_id}-{unit_id}"
    if unit_name:
        return f"{base} {unit_name}"
    return base


def _esa_tag_matches(name: Optional[str], site_id: int, unit_id: int) -> bool:
    """True if `name` is an ESA tag owned by (site_id, unit_id), regardless
    of any trailing unit-name suffix.
    """
    if not name:
        return False
    base = f"ESA-{site_id}-{unit_id}"
    return name == base or name.startswith(base + ' ')


class _PolicyDefaults:
    """Sentinel for missing SmartLockSiteConfig rows — all flags TRUE (secure)."""
    revoke_on_gate_locked = True
    revoke_on_overlocked = True
    revoke_on_not_rentable = True


def _revoke_reason(
    is_rented: bool,
    b_rentable: bool,
    is_gate_locked: bool,
    is_overlocked: bool,
    policy: Any,
) -> str:
    """Return the first-match revoke reason using precedence order.

    Precedence: not_rentable -> overlocked -> gate_locked -> moved_out
    Caller is responsible for only calling when should_have_pin is False.
    """
    if policy.revoke_on_not_rentable and not b_rentable:
        return 'not_rentable'
    if policy.revoke_on_overlocked and is_overlocked:
        return 'overlocked'
    if policy.revoke_on_gate_locked and is_gate_locked:
        return 'gate_locked'
    return 'moved_out'


class IglooPinSyncPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        from common.igloo_client import (
            IglooClient, IglooAPIError, IglooBridgePendingError,
            IglooBridgeOfflineError, PIN_TYPE_PERMANENT,
        )
        from common.gate_access_crypto import get_gate_crypto

        dry_run: bool = bool(scope.get('dry_run', False))
        site_id_filter: Optional[List[int]] = scope.get('site_ids')

        # Counters
        sites_processed = 0
        units_checked = 0
        pushed = 0
        revoked = 0
        revoked_not_rentable = 0
        revoked_overlocked = 0
        revoked_gate_locked = 0
        revoked_moved_out = 0
        skipped_no_gate_code = 0
        skipped_invalid_pin = 0
        skipped_site_disabled = 0
        skipped_bridge_offline = 0
        # Track devices that have been confirmed offline this run; subsequent
        # units on these devices are skipped without retrying the bridge.
        offline_devices: Set[str] = set()
        skipped_pending_job = 0
        skipped_legacy_in_sync = 0
        errors = 0

        # --- 1) Instantiate Igloo client (fail-fast on missing credentials) ---
        try:
            client = IglooClient()
        except IglooAPIError as e:
            logger.error("IglooPinSyncPipeline: cannot create Igloo client: %s", e)
            return RunResult(
                status='failed',
                records=0,
                scope=scope,
                error='Igloo credentials missing or invalid',
            )

        crypto = get_gate_crypto()
        engine = get_engine('middleware')

        # --- 2) Load enabled sites, policy configs, and assignments ---
        with session_scope('middleware') as session:
            from web.models.smart_lock import (
                SmartLockUnitAssignment,
                SmartLockKeypad,
                SmartLockSiteConfig,
                GateAccessData,
                SmartLockAuditLog,
            )

            # Load all site configs into a keyed dict.
            # Missing row -> _PolicyDefaults (all flags TRUE, enabled).
            site_configs: Dict[int, Any] = {}
            disabled_site_ids: Set[int] = set()
            for cfg in session.query(SmartLockSiteConfig).all():
                site_configs[cfg.site_id] = cfg
                if not cfg.enabled:
                    disabled_site_ids.add(cfg.site_id)

            # Assignments with at least one keypad
            q = session.query(SmartLockUnitAssignment).filter(
                (SmartLockUnitAssignment.keypad_pk.isnot(None)) |
                (SmartLockUnitAssignment.keypad_2_pk.isnot(None))
            )
            if site_id_filter:
                q = q.filter(SmartLockUnitAssignment.site_id.in_(site_id_filter))
            assignments = q.all()

            if not assignments:
                return RunResult(
                    status='refreshed', records=0, scope=scope,
                    metadata={
                        'sites_processed': 0, 'units_checked': 0,
                        'pushed': 0, 'revoked': 0,
                        'revoked_not_rentable': 0, 'revoked_overlocked': 0,
                        'revoked_gate_locked': 0, 'revoked_moved_out': 0,
                        'skipped_no_gate_code': 0, 'skipped_invalid_pin': 0,
                        'skipped_site_disabled': 0, 'skipped_pending_job': 0,
                        'skipped_legacy_in_sync': 0,
                        'skipped_bridge_offline': 0,
                        'offline_devices': [],
                        'errors': 0, 'dry_run': dry_run,
                    },
                )

            # Build keypad PK -> keypad_id map for all relevant keypad PKs
            all_kp_pks: Set[int] = set()
            for a in assignments:
                if a.keypad_pk:
                    all_kp_pks.add(a.keypad_pk)
                if a.keypad_2_pk:
                    all_kp_pks.add(a.keypad_2_pk)

            keypads = session.query(SmartLockKeypad).filter(
                SmartLockKeypad.id.in_(all_kp_pks)
            ).all()
            kp_map: Dict[int, str] = {k.id: k.keypad_id for k in keypads}

            # Collect all site_ids we'll work against
            active_site_ids: Set[int] = set()
            for a in assignments:
                if a.site_id not in disabled_site_ids:
                    active_site_ids.add(a.site_id)

            # Load gate access rows for all active sites in one query
            gate_rows = session.query(GateAccessData).filter(
                GateAccessData.site_id.in_(active_site_ids)
            ).all()
            gate_map: Dict[Tuple[int, int], GateAccessData] = {
                (g.site_id, g.unit_id): g for g in gate_rows
            }

            # Load bRentable + bRented from ccws_units for active sites.
            # ccws_units.bRented is the authoritative occupancy signal —
            # ccws_gate_access.is_rented is unreliable because some sites
            # delete the gate-access enrollment on move-out (SiteLink-side
            # behavior), leaving stale is_rented=true rows in our DB.
            # Missing row -> bRentable=False, bRented=False (fail-safe: no PIN).
            from sqlalchemy import text as _text
            rentable_map: Dict[Tuple[int, int], bool] = {}
            rented_map: Dict[Tuple[int, int], bool] = {}
            if active_site_ids:
                rows = session.execute(
                    _text(
                        'SELECT "SiteID", "UnitID", "bRentable", "bRented" '
                        'FROM ccws_units WHERE "SiteID" = ANY(:sids) '
                        'AND deleted_at IS NULL'
                    ),
                    {'sids': list(active_site_ids)},
                ).fetchall()
                for sid, uid, rentable, rented in rows:
                    rentable_map[(sid, uid)] = bool(rentable)
                    rented_map[(sid, uid)] = bool(rented)

            # --- 3) Pre-fetch Igloo device state (one call per unique device_id) ---
            all_device_ids: Set[str] = set(kp_map[pk] for pk in all_kp_pks if pk in kp_map)

            # device_access[device_id] = list of access entry dicts
            device_access: Dict[str, List[Dict[str, Any]]] = {}
            # device_pending[device_id] = set of pending customPin values
            device_pending: Dict[str, Set[str]] = {}

            for device_id in all_device_ids:
                try:
                    access_list = client.list_device_access(device_id)
                    device_access[device_id] = access_list
                except IglooAPIError:
                    logger.error(
                        "IglooPinSyncPipeline: failed to read access for device %s", device_id
                    )
                    device_access[device_id] = []
                    errors += 1

                try:
                    jobs = client.list_device_jobs(device_id)
                    pending_pins: Set[str] = set()
                    for j in jobs:
                        if j.get('status') != 'pending':
                            continue
                        # Direct device job: customPin in accessData
                        if j.get('description') == 'create_bluetooth_pin':
                            cp = (j.get('accessData') or {}).get('customPin')
                            if cp:
                                pending_pins.add(cp)
                            continue
                        # Bridge-proxied create-pin job (jobType=4). Distinct
                        # shape: pin lives under jobData.pin.
                        if j.get('jobType') == 4:
                            jd = j.get('jobData') or {}
                            cp = jd.get('pin') or jd.get('customPin')
                            if cp:
                                pending_pins.add(cp)
                    device_pending[device_id] = pending_pins
                except IglooAPIError:
                    logger.error(
                        "IglooPinSyncPipeline: failed to read jobs for device %s", device_id
                    )
                    device_pending[device_id] = set()
                    errors += 1

            # --- 4) Reconcile each assignment ---
            processed_sites: Set[int] = set()
            audit_rows: List[SmartLockAuditLog] = []

            for assignment in assignments:
                site_id = assignment.site_id
                unit_id = assignment.unit_id

                if site_id in disabled_site_ids:
                    skipped_site_disabled += 1
                    continue

                processed_sites.add(site_id)
                units_checked += 1

                # Resolve per-site policy (missing row -> secure defaults)
                policy = site_configs.get(site_id, _PolicyDefaults())

                gate = gate_map.get((site_id, unit_id))

                # Build list of device_ids for this assignment
                device_slots: List[str] = []
                if assignment.keypad_pk and assignment.keypad_pk in kp_map:
                    device_slots.append(kp_map[assignment.keypad_pk])
                if assignment.keypad_2_pk and assignment.keypad_2_pk in kp_map:
                    device_slots.append(kp_map[assignment.keypad_2_pk])

                if not device_slots:
                    continue

                tag = _esa_tag(site_id, unit_id, gate.unit_name if gate else None)

                # Resolve gate code (or None)
                plain_pin: Optional[str] = None
                if gate and gate.access_code_enc:
                    try:
                        plain_pin = crypto.decrypt(gate.access_code_enc)
                    except Exception:
                        logger.warning(
                            "IglooPinSyncPipeline: decrypt failed site=%s unit=%s",
                            site_id, unit_id,
                        )

                # Occupancy from ccws_units (source of truth), not gate_access.
                is_rented = rented_map.get((site_id, unit_id), False)
                is_gate_locked = bool(gate and gate.is_gate_locked)
                is_overlocked = bool(gate and gate.is_overlocked)
                b_rentable = rentable_map.get((site_id, unit_id), False)
                has_valid_gate_code = bool(plain_pin and _PIN_RE.match(plain_pin))

                # Compute should_have_pin (gate code validity handled per-device below)
                should_have_pin_base = (
                    is_rented
                    and (not policy.revoke_on_not_rentable or b_rentable)
                    and (not policy.revoke_on_gate_locked or not is_gate_locked)
                    and (not policy.revoke_on_overlocked or not is_overlocked)
                )

                for device_id in device_slots:
                    # Bridge offline short-circuit: if a previous unit on this
                    # device hit a "bridge offline" 406, don't keep retrying.
                    if device_id in offline_devices:
                        skipped_bridge_offline += 1
                        continue

                    access_list = device_access.get(device_id, [])
                    pending_set = device_pending.get(device_id, set())

                    # Find existing ESA-tagged entry for this unit (prefix match
                    # so changes to the unit_name suffix don't lose ownership).
                    existing_esa: Optional[Dict[str, Any]] = None
                    for entry in access_list:
                        if _esa_tag_matches(entry.get('name'), site_id, unit_id):
                            existing_esa = entry
                            break

                    # Full should_have_pin includes valid gate code requirement
                    should_have_pin = should_have_pin_base and has_valid_gate_code

                    if should_have_pin_base and not has_valid_gate_code:
                        # Unit passes policy checks but has no usable PIN
                        if not plain_pin:
                            skipped_no_gate_code += 1
                            logger.debug(
                                "skip site=%s unit=%s device=%s: no gate code",
                                site_id, unit_id, device_id,
                            )
                        else:
                            skipped_invalid_pin += 1
                            logger.warning(
                                "skip site=%s unit=%s device=%s: invalid PIN format",
                                site_id, unit_id, device_id,
                            )
                            audit_rows.append(SmartLockAuditLog(
                                action='pin_invalid_format',
                                entity_type='igloo',
                                entity_id=device_id,
                                site_id=site_id,
                                unit_id=unit_id,
                                detail=(
                                    f"unit {unit_id} gate code is invalid "
                                    f"(must be 4-10 digits) — fix in SiteLink"
                                ),
                                username='orchestrator',
                            ))
                        continue

                    if should_have_pin:
                        existing_pin_value = (
                            existing_esa.get('pin') if existing_esa else None
                        )

                        if existing_pin_value == plain_pin:
                            # Already in sync — skip
                            continue

                        # Check pending jobs before pushing
                        if plain_pin in pending_set:
                            skipped_pending_job += 1
                            logger.debug(
                                "skip site=%s unit=%s device=%s: push already pending",
                                site_id, unit_id, device_id,
                            )
                            continue

                        # Legacy-aware check: if ANY PIN on the device already has
                        # this value (even with a non-ESA description), Igloo's
                        # server-side uniqueness constraint will reject the push
                        # with 409 "custom.pin.already.exists". Skip quietly — the
                        # gate code is effectively live on the device; we simply
                        # don't own it. On move-out we won't be able to revoke it
                        # either (no ESA tag), but that's acceptable per the
                        # "ignore legacy" policy.
                        if any(
                            e.get('pin') == plain_pin
                            and not _esa_tag_matches(e.get('name'), site_id, unit_id)
                            for e in access_list
                        ):
                            skipped_legacy_in_sync += 1
                            audit_rows.append(SmartLockAuditLog(
                                action='pin_collision_with_legacy',
                                entity_type='igloo',
                                entity_id=device_id,
                                site_id=site_id,
                                unit_id=unit_id,
                                detail=(
                                    f"unit {unit_id} gate code already exists on "
                                    f"keypad as a non-ESA entry — manual review"
                                ),
                                username='orchestrator',
                            ))
                            logger.debug(
                                "skip site=%s unit=%s device=%s: legacy PIN already matches",
                                site_id, unit_id, device_id,
                            )
                            continue

                        # Revoke stale ESA-tagged PIN first (if present with wrong value)
                        if existing_esa:
                            access_id = existing_esa.get('id') or existing_esa.get('accessId', '')
                            if access_id:
                                try:
                                    if not dry_run:
                                        client.delete_pin_via_bridge(device_id, access_id)
                                    detail = (
                                        f"{'[DRY RUN] ' if dry_run else ''}"
                                        f"auto-sync revoke: stale PIN replaced for unit {unit_id}"
                                    )
                                    audit_rows.append(SmartLockAuditLog(
                                        action='pin_auto_revoked',
                                        entity_type='igloo',
                                        entity_id=device_id,
                                        site_id=site_id,
                                        unit_id=unit_id,
                                        detail=detail[:500],
                                        username='orchestrator',
                                    ))
                                    revoked += 1
                                except IglooAPIError:
                                    logger.error(
                                        "IglooPinSyncPipeline: revoke failed "
                                        "device=%s access_id=%s site=%s unit=%s",
                                        device_id, access_id, site_id, unit_id,
                                    )
                                    errors += 1
                                    continue  # don't push if revoke failed

                        # Push new PIN (permanent, via bridge)
                        try:
                            if not dry_run:
                                client.create_pin_via_bridge(
                                    device_id, plain_pin, tag,
                                    pin_type=PIN_TYPE_PERMANENT,
                                )
                            detail = (
                                f"{'[DRY RUN] ' if dry_run else ''}"
                                f"auto-sync push: "
                                f"{'no existing PIN' if not existing_esa else 'PIN updated'}"
                            )
                            audit_rows.append(SmartLockAuditLog(
                                action='pin_auto_pushed',
                                entity_type='igloo',
                                entity_id=device_id,
                                site_id=site_id,
                                unit_id=unit_id,
                                detail=detail[:500],
                                username='orchestrator',
                            ))
                            pushed += 1
                            # Append to in-memory access_list so subsequent
                            # units sharing the same plain_pin (multi-tenant
                            # case: same person rents multiple units) hit the
                            # `skipped_legacy_in_sync` branch instead of failing
                            # the duplicate push.
                            access_list.append({
                                'name': tag,
                                'pin': plain_pin,
                                'pinType': 'permanent',
                            })
                        except IglooBridgePendingError:
                            # Igloo already has a pending bridge job for this
                            # PIN — benign, will resolve on a subsequent cycle.
                            skipped_pending_job += 1
                            logger.debug(
                                "skip site=%s unit=%s device=%s: bridge job pending",
                                site_id, unit_id, device_id,
                            )
                        except IglooBridgeOfflineError as exc:
                            # Bridge unreachable. Mark device offline so we
                            # don't keep retrying for the rest of this run, and
                            # write a single audit row per device-run.
                            if device_id not in offline_devices:
                                offline_devices.add(device_id)
                                logger.warning(
                                    "Bridge offline for device=%s site=%s — "
                                    "skipping remaining units this run",
                                    device_id, site_id,
                                )
                                audit_rows.append(SmartLockAuditLog(
                                    action='bridge_offline',
                                    entity_type='igloo',
                                    entity_id=device_id,
                                    site_id=site_id,
                                    unit_id=unit_id,
                                    detail=(
                                        f"keypad bridge offline — "
                                        f"on-site network/power check needed"
                                    ),
                                    username='orchestrator',
                                ))
                            skipped_bridge_offline += 1
                        except IglooAPIError as exc:
                            logger.error(
                                "IglooPinSyncPipeline: push failed "
                                "device=%s site=%s unit=%s err=%s",
                                device_id, site_id, unit_id, exc,
                            )
                            audit_rows.append(SmartLockAuditLog(
                                action='pin_push_failed',
                                entity_type='igloo',
                                entity_id=device_id,
                                site_id=site_id,
                                unit_id=unit_id,
                                detail=f"push error: {_redact_digits(str(exc))[:400]}",
                                username='orchestrator',
                            ))
                            errors += 1

                    else:
                        # should_have_pin = False — revoke ESA-tagged PIN if present.
                        if not existing_esa:
                            # No ESA-owned entry. But if the device has a
                            # legacy (non-ESA) entry whose PIN matches the
                            # current gate code, that's a security gap on
                            # move-out: the tenant's PIN is live and we can't
                            # revoke it (no accessId we own). Surface as a
                            # warning + audit row so operators can clean up.
                            if plain_pin and any(
                                e.get('pin') == plain_pin
                                and not _esa_tag_matches(e.get('name'), site_id, unit_id)
                                for e in access_list
                            ):
                                logger.warning(
                                    "IglooPinSyncPipeline: legacy PIN not revoked "
                                    "on move-out site=%s unit=%s device=%s",
                                    site_id, unit_id, device_id,
                                )
                                audit_rows.append(SmartLockAuditLog(
                                    action='pin_revoke_skipped_legacy',
                                    entity_type='igloo',
                                    entity_id=device_id,
                                    site_id=site_id,
                                    unit_id=unit_id,
                                    detail=(
                                        f"unit {unit_id} vacated but PIN is on a "
                                        f"non-ESA entry — manual revoke required"
                                    ),
                                    username='orchestrator',
                                ))
                            continue

                        access_id = existing_esa.get('id') or existing_esa.get('accessId', '')
                        if not access_id:
                            logger.warning(
                                "IglooPinSyncPipeline: ESA entry without accessId "
                                "site=%s unit=%s device=%s — cannot revoke",
                                site_id, unit_id, device_id,
                            )
                            continue

                        reason = _revoke_reason(
                            is_rented, b_rentable, is_gate_locked, is_overlocked, policy
                        )

                        try:
                            if not dry_run:
                                client.delete_pin_via_bridge(device_id, access_id)
                            detail = (
                                f"{'[DRY RUN] ' if dry_run else ''}"
                                f"auto-sync revoke: {reason} for unit {unit_id}"
                            )
                            audit_rows.append(SmartLockAuditLog(
                                action='pin_auto_revoked',
                                entity_type='igloo',
                                entity_id=device_id,
                                site_id=site_id,
                                unit_id=unit_id,
                                detail=detail[:500],
                                username='orchestrator',
                            ))
                            revoked += 1
                            if reason == 'not_rentable':
                                revoked_not_rentable += 1
                            elif reason == 'overlocked':
                                revoked_overlocked += 1
                            elif reason == 'gate_locked':
                                revoked_gate_locked += 1
                            else:
                                revoked_moved_out += 1
                        except IglooBridgeOfflineError:
                            if device_id not in offline_devices:
                                offline_devices.add(device_id)
                                logger.warning(
                                    "Bridge offline (revoke path) device=%s site=%s",
                                    device_id, site_id,
                                )
                                audit_rows.append(SmartLockAuditLog(
                                    action='bridge_offline',
                                    entity_type='igloo',
                                    entity_id=device_id,
                                    site_id=site_id,
                                    unit_id=unit_id,
                                    detail=(
                                        f"keypad bridge offline during revoke — "
                                        f"PIN remains active until bridge restored"
                                    ),
                                    username='orchestrator',
                                ))
                            skipped_bridge_offline += 1
                        except IglooAPIError as exc:
                            logger.error(
                                "IglooPinSyncPipeline: revoke (%s) failed "
                                "device=%s access_id=%s site=%s unit=%s err=%s",
                                reason, device_id, access_id, site_id, unit_id, exc,
                            )
                            audit_rows.append(SmartLockAuditLog(
                                action='pin_revoke_failed',
                                entity_type='igloo',
                                entity_id=device_id,
                                site_id=site_id,
                                unit_id=unit_id,
                                detail=f"revoke ({reason}) error: {_redact_digits(str(exc))[:380]}",
                                username='orchestrator',
                            ))
                            errors += 1

            # --- 5) Flush audit log rows ---
            if audit_rows:
                try:
                    session.add_all(audit_rows)
                    session.commit()
                except Exception:
                    logger.exception("IglooPinSyncPipeline: failed to write audit log")
                    session.rollback()

            sites_processed = len(processed_sites)

        return RunResult(
            status='refreshed',
            records=units_checked,
            scope=scope,
            metadata={
                'sites_processed': sites_processed,
                'units_checked': units_checked,
                'pushed': pushed,
                'revoked': revoked,
                'revoked_not_rentable': revoked_not_rentable,
                'revoked_overlocked': revoked_overlocked,
                'revoked_gate_locked': revoked_gate_locked,
                'revoked_moved_out': revoked_moved_out,
                'skipped_no_gate_code': skipped_no_gate_code,
                'skipped_invalid_pin': skipped_invalid_pin,
                'skipped_site_disabled': skipped_site_disabled,
                'skipped_pending_job': skipped_pending_job,
                'skipped_legacy_in_sync': skipped_legacy_in_sync,
                'skipped_bridge_offline': skipped_bridge_offline,
                'offline_devices': sorted(offline_devices),
                'errors': errors,
                'dry_run': dry_run,
            },
        )
