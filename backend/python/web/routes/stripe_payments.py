"""
Stripe payment routes — scaffolding for booking engine / chatbot payment flow.

Supports PaymentIntent creation, retrieval, and webhook handling.
Move-in bridge (triggered on payment_intent.succeeded) is intentionally
left as TODO — wire in once the lead↔reservation join logic is reviewed.
"""

import logging
from datetime import datetime

import stripe
from flask import Blueprint, jsonify, request, current_app
from sqlalchemy.exc import IntegrityError

from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api
from web.utils.audit import audit_log, AuditEvent

logger = logging.getLogger(__name__)

stripe_bp = Blueprint('stripe_payments', __name__, url_prefix='/api/stripe')

# Lazy-loaded Stripe config
_stripe_initialised = False


def _init_stripe():
    global _stripe_initialised
    if not _stripe_initialised:
        from common.secrets_vault import vault_config
        stripe.api_key = vault_config('STRIPE_SECRET_KEY')
        _stripe_initialised = True


def _get_webhook_secret():
    from common.secrets_vault import vault_config
    return vault_config('STRIPE_WEBHOOK_SECRET')


# =============================================================================
# Payment Intents
# =============================================================================

@stripe_bp.route('/payment-intents', methods=['POST'])
@require_auth
@require_api_scope('payments:write')
@rate_limit_api(max_requests=20, window_seconds=60)
def create_payment_intent():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    amount = data.get('amount')
    if not isinstance(amount, int) or amount <= 0:
        return jsonify({'error': 'amount must be a positive integer (cents)'}), 400

    currency = data.get('currency', 'sgd').lower()
    metadata = data.get('metadata') or {}
    if not isinstance(metadata, dict):
        return jsonify({'error': 'metadata must be an object'}), 400

    try:
        _init_stripe()

        intent = stripe.PaymentIntent.create(
            amount=amount,
            currency=currency,
            metadata=metadata,
        )

        audit_log(
            AuditEvent.STRIPE_PAYMENT_INTENT_CREATED,
            f"Created PaymentIntent: id={intent.id}, amount={amount} {currency}",
        )

        return jsonify({
            'status': 'success',
            'data': {
                'id': intent.id,
                'client_secret': intent.client_secret,
                'amount': intent.amount,
                'currency': intent.currency,
                'payment_status': intent.status,
            }
        }), 201

    except stripe.error.StripeError as e:
        logger.error("Stripe PaymentIntent create failed: %s", e.user_message or str(e))
        return jsonify({'error': 'Failed to create payment intent'}), 502
    except Exception:
        logger.exception("Stripe PaymentIntent create error")
        return jsonify({'error': 'Failed to create payment intent'}), 500


@stripe_bp.route('/payment-intents/<intent_id>', methods=['GET'])
@require_auth
@require_api_scope('payments:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def get_payment_intent(intent_id):
    if not intent_id.startswith('pi_'):
        return jsonify({'error': 'Invalid payment intent ID format'}), 400

    try:
        _init_stripe()

        intent = stripe.PaymentIntent.retrieve(intent_id)

        return jsonify({
            'status': 'success',
            'data': {
                'id': intent.id,
                'amount': intent.amount,
                'currency': intent.currency,
                'payment_status': intent.status,
                'metadata': dict(intent.metadata),
            }
        })

    except stripe.error.InvalidRequestError as e:
        logger.error("Stripe PaymentIntent retrieve failed: %s", str(e))
        return jsonify({'error': 'Payment intent not found'}), 404
    except stripe.error.StripeError as e:
        logger.error("Stripe PaymentIntent retrieve error: %s", e.user_message or str(e))
        return jsonify({'error': 'Failed to retrieve payment intent'}), 502
    except Exception:
        logger.exception("Stripe PaymentIntent retrieve error")
        return jsonify({'error': 'Failed to retrieve payment intent'}), 500


# =============================================================================
# Webhook
# Note: intentionally NOT decorated with @require_auth — Stripe sends no JWT.
# Signature verification via STRIPE_WEBHOOK_SECRET replaces authentication.
# =============================================================================

@stripe_bp.route('/webhook', methods=['POST'])
@rate_limit_api(max_requests=120, window_seconds=60)
def stripe_webhook():
    payload = request.get_data()
    sig_header = request.headers.get('Stripe-Signature', '')

    try:
        webhook_secret = _get_webhook_secret()
        event = stripe.Webhook.construct_event(payload, sig_header, webhook_secret)
    except ValueError:
        logger.warning("Stripe webhook: invalid payload")
        return jsonify({'error': 'Invalid payload'}), 400
    except stripe.error.SignatureVerificationError:
        logger.warning("Stripe webhook: signature verification failed")
        return jsonify({'error': 'Invalid signature'}), 400
    except Exception:
        logger.exception("Stripe webhook: setup error")
        return jsonify({'error': 'Webhook configuration error'}), 500

    event_id = event.get('id', 'unknown')
    event_type = event.get('type', 'unknown')
    payment_intent_id = None
    if event_type.startswith('payment_intent.'):
        payment_intent_id = event['data']['object'].get('id')

    logger.info("Stripe webhook received: type=%s id=%s", event_type, event_id)

    audit_log(
        AuditEvent.STRIPE_WEBHOOK_RECEIVED,
        f"Stripe webhook: type={event_type}, id={event_id}",
    )

    # ------------------------------------------------------------------
    # Idempotency guard — INSERT first, process only on first delivery.
    # Stripe retries webhooks for up to 3 days; duplicate deliveries must
    # not trigger duplicate SOAP writes or double move-ins.
    #
    # Contract:
    #   - INSERT a row with status='received' keyed on event_id (evt_xxx).
    #   - If the INSERT violates the UNIQUE constraint the event was already
    #     seen (delivered/retried); return 200 immediately so Stripe stops
    #     retrying, but skip all business logic.
    #   - On successful first INSERT, run business logic inside the same
    #     db session, then UPDATE status='processed' / processed_at=now().
    #   - On exception, UPDATE status='failed' + error_message, then re-raise
    #     or return 500 so Stripe retries (only safe if the SOAP call is
    #     confirmed to have NOT been applied — handle accordingly).
    # ------------------------------------------------------------------
    from common.models import StripeWebhookEvent

    db = current_app.get_db_session()
    try:
        webhook_row = StripeWebhookEvent(
            event_id=event_id,
            event_type=event_type,
            payment_intent_id=payment_intent_id,
            status='received',
        )
        db.add(webhook_row)
        db.flush()   # raises IntegrityError immediately on UNIQUE conflict
    except IntegrityError:
        db.rollback()
        logger.info(
            "Stripe webhook duplicate — already processed: type=%s id=%s",
            event_type, event_id,
        )
        return jsonify({'status': 'ok', 'note': 'duplicate'}), 200
    finally:
        # Keep the session open below only when flush succeeded (no exception).
        # On IntegrityError we already returned, so this path only runs in the
        # success branch.
        pass

    try:
        if event_type == 'payment_intent.succeeded':
            intent = event['data']['object']
            meta = dict(intent.get('metadata', {}))
            logger.info(
                "PaymentIntent succeeded: id=%s amount=%s currency=%s metadata=%s",
                intent.get('id'), intent.get('amount'), intent.get('currency'), meta,
            )

            # TODO (Flow B): trigger move-in cash bypass here.
            # Metadata expected from chatbot: reservation_id, lead_id, site_id, unit_id.
            # Insert the MoveInReservation_v6 SOAP call in this block:
            #
            #   result = initiate_move_in_cash_bypass(
            #       reservation_id=meta.get('reservation_id'),
            #       lead_id=meta.get('lead_id'),
            #       site_id=meta.get('site_id'),
            #       unit_id=meta.get('unit_id'),
            #       stripe_payment_intent_id=intent.get('id'),
            #   )
            #
            # On success: fall through to the processed commit below.
            # On SOAP error: raise an exception so the except block marks
            # status='failed' — only do this if the SOAP call is safe to
            # retry (i.e. the move-in was NOT applied); otherwise mark
            # processed to prevent a double move-in on Stripe retry.

        elif event_type == 'payment_intent.payment_failed':
            intent = event['data']['object']
            failure = intent.get('last_payment_error', {})
            logger.warning(
                "PaymentIntent failed: id=%s code=%s message=%s",
                intent.get('id'),
                failure.get('code'),
                failure.get('message'),
            )
            # TODO: notify chatbot / lead system of payment failure if needed.

        # Mark event as successfully handled
        webhook_row.status = 'processed'
        webhook_row.processed_at = datetime.utcnow()
        db.commit()

    except Exception:
        logger.exception(
            "Stripe webhook processing error: type=%s id=%s", event_type, event_id
        )
        try:
            webhook_row.status = 'failed'
            webhook_row.error_message = 'Internal processing error'
            db.commit()
        except Exception:
            db.rollback()
        return jsonify({'error': 'Webhook processing failed'}), 500
    finally:
        db.close()

    # Return 200 for all handled event types
    return jsonify({'status': 'ok'}), 200
