"""Discount Plan model for managing storage discount/promotion plans."""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, Date, Numeric
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from web.models.base import Base


class DiscountPlan(Base):
    """
    Discount Plan definitions for Extra Space Asia.

    Replaces the Excel-based discount plan tracking sheet with a proper
    database-backed system. Each row represents a distinct promotional
    offer or pricing plan (e.g., Prepaid 6M, Staff Rate, Referral).

    Supports both:
    - Basic rate plan fields (Plan Type, Discount %, Sites, T&Cs...)
    - Promotion brief fields (Offers, ChatBot, Distribution, bilingual T&Cs...)
    """
    __tablename__ = 'mw_discount_plans'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # =========================================================================
    # Plan Identification
    # =========================================================================
    plan_type = Column(String(50), nullable=False, comment="Category: Evergreen, Tactical, Seasonal")
    plan_name = Column(String(255), nullable=False, unique=True, comment="Unique plan name")
    group_name = Column(String(255), index=True, comment="Bucket label; plans sharing a group_name are managed together on the list page")

    # =========================================================================
    # Description
    # =========================================================================
    notes = Column(Text, comment="Internal notes about the plan")
    objective = Column(Text, comment="Business objective of this plan")

    # =========================================================================
    # Availability / Scheduling
    # =========================================================================
    period_range = Column(String(255), comment="Offer validity, e.g. Permanent, From X till Y")
    period_start = Column(Date, comment="Offer start date (if time-limited)")
    period_end = Column(Date, comment="Offer end date (if time-limited)")
    promo_period_start = Column(Date, comment="Promotion period start date")
    promo_period_end = Column(Date, comment="Promotion period end date")
    booking_period_start = Column(Date, comment="Booking period start date")
    booking_period_end = Column(Date, comment="Booking period end date")
    move_in_range = Column(String(255), comment="Move-in date constraint")
    applicable_sites = Column(JSONB, comment="Site applicability, e.g. {L001: true, L003: false}")

    # =========================================================================
    # Storage Type
    # =========================================================================
    storage_type = Column(String(50), comment="Storage type: Self Storage, Wine Storage, etc.")

    # =========================================================================
    # Discount Details
    # =========================================================================
    discount_value = Column(String(255), comment="Discount description, e.g. 5%, 300HKD, First 2 Weeks Free")
    discount_type = Column(String(50), comment="percentage, fixed_amount, free_period, or none")
    discount_numeric = Column(Numeric(10, 2), comment="Numeric discount value for calculations")
    discount_segmentation = Column(String(100), comment="Margin segmentation, e.g. >=5% < 10%")
    clawback_condition = Column(Text, comment="Condition if tenant leaves early")

    # Promotion brief: multiple offer tiers
    # e.g. [{"tier": "Flexi", "discount": "45% off"}, {"tier": "LT12M", "discount": "50% off", "note": "with free transportation"}]
    offers = Column(JSONB, comment="Offer tiers for promotion briefs")

    # =========================================================================
    # Terms & Conditions
    # =========================================================================
    deposit = Column(String(255), comment="Deposit requirement, e.g. 1 Month (Refundable)")
    payment_terms = Column(String(100), comment="Payment terms, e.g. Monthly, Prepaid (6M)")
    termination_notice = Column(String(100), comment="Termination notice period, e.g. 1 Month")
    extra_offer = Column(String(255), comment="Additional offer, e.g. -20% Off Merchandise")
    lock_in_period = Column(String(255), comment="Lock-in period, e.g. Minimum 6 months")
    terms_conditions = Column(JSONB, comment="Array of T&C clauses (English)")
    terms_conditions_cn = Column(JSONB, comment="Array of T&C clauses (Chinese)")
    tc_labels = Column(JSONB, comment="Array of T&C clause labels, parallel to terms_conditions")
    terms_conditions_translations = Column(JSONB, default=dict, comment="AI-translated T&Cs keyed by language code")

    # =========================================================================
    # Promotion Brief: Eligibility & Channel
    # =========================================================================
    hidden_rate = Column(Boolean, default=False, comment="Whether rate is hidden from public")
    coupon_code = Column(String(100), comment="Unlock code for hidden_rate plans; ignored when hidden_rate is False")
    discount_perpetual = Column(
        Boolean, default=False, nullable=False,
        comment="When TRUE, the recommender's calculator applies the discount to "
                "every billing month of the lease (not just iInMonth=1). Used for "
                "concessions where ops staff manually click 'Apply Tenant's Rate' "
                "at move-in to make the discount perpetual. Part 2 will automate "
                "the rate write so this flag eventually becomes a hint to the bot "
                "rather than a manual workaround indicator.",
    )
    # Phase 4 Part 2 — perpetual + prepay orchestration.
    prepayment_months = Column(
        Integer, nullable=True,
        comment="N months prepaid upfront at the discounted rate. NULL = no prepay required. "
                "When set, /api/reservations/move-in pushes the surplus via SOAP "
                "PaymentSimpleCash so SiteLink's dPaidThru advances by exactly N months. "
                "Schedules the post-prepay rate change at move_in_date + N.",
    )
    post_prepay_ecri_pct = Column(
        Numeric(5, 2), nullable=True,
        comment="Plan-level override for the ECRI uplift % applied at the end of the "
                "prepay window. NULL = use the global ecri_default_pct from "
                "mw_recommender_settings.",
    )
    available_for_chatbot = Column(Boolean, default=False, comment="Available for chatbot promotion")
    chatbot_notes = Column(String(255), comment="ChatBot availability notes")
    switch_to_us = Column(String(50), default='Not Eligible', comment="Switch-To-Us eligibility")
    referral_program = Column(String(50), default='Not Eligible', comment="Referral program eligibility")
    distribution_channel = Column(String(255), comment="Distribution channel, e.g. Direct Mailing, Online")

    # =========================================================================
    # Departmental Info
    # =========================================================================
    # REV: Rate rules applicable per site
    rate_rules = Column(Text, comment="Rate rules for REV department")
    rate_rules_sites = Column(String(500), comment="Sites for rate rules")

    # OPS & SALES: Promotion codes
    promotion_codes = Column(JSONB, comment="Promotion codes list, e.g. ['Direct Mail Flexi 45% off']")

    # MKG: Collateral and registration flow
    collateral_url = Column(Text, comment="URL to marketing collateral (flyer, PDF)")
    registration_flow = Column(Text, comment="Description of registration/conversion flow")

    # General department notes (flexible JSONB for per-dept remarks)
    department_notes = Column(JSONB, comment="Per-department notes, e.g. {REV: '...', OPS: '...', MKG: '...'}")

    # =========================================================================
    # =========================================================================
    # Sitelink Concession Linking
    # =========================================================================
    linked_concessions = Column(JSONB, default=list, comment="Array of {site_id, concession_id} pairs linking to ccws_discount")
    # When TRUE, the plan is a "Standard Rate" pseudo-plan — booking flow
    # sends ConcessionID=0 to SOAP (no discount). Candidates pipeline emits
    # rows with concession-derived fields nulled and effective_rate=std_rate.
    is_stdrate_override = Column(Boolean, nullable=False, default=False,
                                 comment="Force ConcessionID=0; bypass ccws_discount lookup")

    # =========================================================================
    # Unit-level Restrictions (SOP COM01 dims)
    # =========================================================================
    # Dict keyed by dim name (size_category, size_range, unit_type, climate_type,
    # unit_shape, pillar) with value = list of allowed codes. Empty list OR
    # missing key = "no restriction on that dim".
    restrictions = Column(JSONB, default=dict, comment="Per-dim multi-select restrictions from SOP COM01")

    # =========================================================================
    # Extensible Custom Fields
    # =========================================================================
    # =========================================================================
    # Status & Ordering
    # =========================================================================
    is_active = Column(Boolean, nullable=False, default=True, comment="Whether plan is currently active")
    sort_order = Column(Integer, default=0, comment="Display sort order")

    # =========================================================================
    # Audit
    # =========================================================================
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    created_by = Column(String(255))
    updated_by = Column(String(255))

    def to_dict(self):
        """Convert to dictionary for API responses."""
        return {
            'id': self.id,
            # Identification
            'plan_type': self.plan_type,
            'plan_name': self.plan_name,
            'group_name': self.group_name,
            # Description
            'notes': self.notes,
            'objective': self.objective,
            # Availability
            'period_range': self.period_range,
            'period_start': self.period_start.isoformat() if self.period_start else None,
            'period_end': self.period_end.isoformat() if self.period_end else None,
            'promo_period_start': self.promo_period_start.isoformat() if self.promo_period_start else None,
            'promo_period_end': self.promo_period_end.isoformat() if self.promo_period_end else None,
            'booking_period_start': self.booking_period_start.isoformat() if self.booking_period_start else None,
            'booking_period_end': self.booking_period_end.isoformat() if self.booking_period_end else None,
            'move_in_range': self.move_in_range,
            'applicable_sites': self.applicable_sites or {},
            # Storage type
            'storage_type': self.storage_type,
            # Discount
            'discount_value': self.discount_value,
            'discount_type': self.discount_type,
            'discount_numeric': float(self.discount_numeric) if self.discount_numeric is not None else None,
            'discount_segmentation': self.discount_segmentation,
            'clawback_condition': self.clawback_condition,
            'offers': self.offers or [],
            # Terms
            'lock_in_period': self.lock_in_period,
            'deposit': self.deposit,
            'payment_terms': self.payment_terms,
            'termination_notice': self.termination_notice,
            'extra_offer': self.extra_offer,
            'terms_conditions': self.terms_conditions or [],
            'terms_conditions_cn': self.terms_conditions_cn or [],
            'tc_labels': self.tc_labels or [],
            'terms_conditions_translations': self.terms_conditions_translations or {},
            # Promotion brief
            'hidden_rate': self.hidden_rate,
            'coupon_code': self.coupon_code,
            'discount_perpetual': bool(self.discount_perpetual),
            'prepayment_months': self.prepayment_months,
            'post_prepay_ecri_pct': float(self.post_prepay_ecri_pct) if self.post_prepay_ecri_pct is not None else None,
            'available_for_chatbot': self.available_for_chatbot,
            'chatbot_notes': self.chatbot_notes,
            'switch_to_us': self.switch_to_us,
            'referral_program': self.referral_program,
            'distribution_channel': self.distribution_channel,
            # Departmental
            'rate_rules': self.rate_rules,
            'rate_rules_sites': self.rate_rules_sites,
            'promotion_codes': self.promotion_codes or [],
            'collateral_url': self.collateral_url,
            'registration_flow': self.registration_flow,
            'department_notes': self.department_notes or {},
            # Sitelink linking
            'linked_concessions': self.linked_concessions or [],
            'is_stdrate_override': bool(self.is_stdrate_override),
            'restrictions': self.restrictions or {},
            # Status
            'is_active': self.is_active,
            'sort_order': self.sort_order,
            # Audit
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'created_by': self.created_by,
            'updated_by': self.updated_by,
        }

    def __repr__(self):
        return f"<DiscountPlan {self.plan_name}>"
