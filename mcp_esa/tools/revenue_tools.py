"""
Revenue Management Tools Module
MCP tools for revenue analytics, budget tracking, and executive reporting.
"""

import asyncio
import json
import logging
from typing import Optional, Dict, TYPE_CHECKING

from mcp.server import Server

if TYPE_CHECKING:
    from mcp_esa.server.mcp_server import MCPServerApp

logger = logging.getLogger(__name__)


# =============================================================================
# Data formatting helpers
# =============================================================================

def _fmt_pct(val, suffix='%'):
    if val is None:
        return 'N/A'
    return f"{val:.2f}{suffix}"


def _fmt_money(val, prefix='$'):
    if val is None:
        return 'N/A'
    return f"{prefix}{val:,.2f}"


def _fmt_num(val):
    if val is None:
        return 'N/A'
    if isinstance(val, float):
        return f"{val:,.2f}"
    return f"{val:,}"


def _format_portfolio_snapshot(snap: dict) -> str:
    if not snap:
        return "No portfolio data available."
    return f"""PORTFOLIO SNAPSHOT ({snap.get('extract_date', 'N/A')}):
- Total Units: {_fmt_num(snap.get('total_units'))}
- Occupied: {_fmt_num(snap.get('occupied'))} | Vacant: {_fmt_num(snap.get('vacant'))}
- Occupancy (Unit): {_fmt_pct(snap.get('occ_pct_unit'))}
- Occupancy (Area): {_fmt_pct(snap.get('occ_pct_area'))}
- Rental Revenue: {_fmt_money(snap.get('total_revenue'))}
- REVPAS: {_fmt_money(snap.get('revpas'))}
- Avg In-Place Rate/Sqf: {_fmt_money(snap.get('avg_inplace_sqft'))}
- Avg STD Rate/Sqf: {_fmt_money(snap.get('avg_std_sqft'))}
- Insurance Penetration: {_fmt_pct(snap.get('insurance_penetration'))}
- Discount Penetration: {_fmt_pct(snap.get('discount_penetration'))}"""


def _format_site_table(sites: list) -> str:
    if not sites:
        return "No site data available."
    lines = [f"SITE PERFORMANCE ({len(sites)} sites):"]
    lines.append(f"{'Site':<8} {'Name':<30} {'Occ%':>6} {'Revenue':>14} {'REVPAS':>8} {'InPlace/Sqf':>12}")
    lines.append("-" * 82)
    for s in sites:
        lines.append(
            f"{s.get('site_code', ''):<8} "
            f"{(s.get('name') or '')[:28]:<30} "
            f"{_fmt_pct(s.get('occ_pct_unit')):>6} "
            f"{_fmt_money(s.get('revenue')):>14} "
            f"{_fmt_money(s.get('revpas')):>8} "
            f"{_fmt_money(s.get('avg_inplace_sqft')):>12}"
        )
    return "\n".join(lines)


def _format_budget_table(rows: list) -> str:
    if not rows:
        return "No budget data available."
    lines = ["BUDGET VARIANCE:"]
    lines.append(f"{'Site':<8} {'Actual':>14} {'Budget':>14} {'Var%':>8} {'Achv%':>8} {'ActOcc%':>8} {'BudOcc%':>8}")
    lines.append("-" * 72)
    for r in rows:
        lines.append(
            f"{r.get('site_code', ''):<8} "
            f"{_fmt_money(r.get('actual_revenue')):>14} "
            f"{_fmt_money(r.get('budget_revenue')):>14} "
            f"{_fmt_pct(r.get('variance_pct')):>8} "
            f"{_fmt_pct(r.get('achievement_pct')):>8} "
            f"{_fmt_pct(r.get('actual_occ')):>8} "
            f"{_fmt_pct(r.get('budget_occ')):>8}"
        )
    return "\n".join(lines)


def _format_movement(data: dict) -> str:
    if not data:
        return "No movement data available."
    return f"""MOVEMENT ANALYSIS:
- Total Move-Ins: {_fmt_num(data.get('total_move_ins'))}
- Total Move-Outs: {_fmt_num(data.get('total_move_outs'))}
- Net Absorption: {_fmt_num(data.get('net_absorption'))}
- Avg Move-In Rate/Sqf: {_fmt_money(data.get('avg_mi_rate_sqft'))}
- Avg Days Vacant Before MI: {_fmt_num(data.get('avg_days_vacant_before_mi'))}
- Avg LOS at Move-Out: {_fmt_num(data.get('avg_los_at_moveout'))} days"""


def _format_anomalies(data: dict) -> str:
    if not data:
        return "No anomaly data available."
    lines = ["ANOMALIES & ALERTS:"]

    occ = data.get('occ_drops', [])
    if occ:
        lines.append(f"\nOccupancy Drops (WoW > 2pp): {len(occ)} sites")
        for r in occ:
            lines.append(f"  - {r.get('site_code')} ({r.get('name')}): {r.get('current_occ_pct')}% (was {r.get('prev_occ_pct')}%, delta {r.get('change_pct')}pp)")
    else:
        lines.append("\nOccupancy Drops: None")

    rev = data.get('revenue_concerns', [])
    if rev:
        lines.append(f"\nRevenue Below Budget (>10% shortfall): {len(rev)} sites")
        for r in rev:
            lines.append(f"  - {r.get('site_code')}: actual {_fmt_money(r.get('actual_revenue'))} vs budget {_fmt_money(r.get('budget_revenue'))} ({r.get('variance_pct')}%)")
    else:
        lines.append("\nRevenue Concerns: None")

    vac = data.get('vacancy_spikes', [])
    if vac:
        lines.append(f"\nLong-Vacant Concentration (>30% of vacant units 60d+): {len(vac)} sites")
        for r in vac:
            lines.append(f"  - {r.get('site_code')} ({r.get('name')}): {r.get('long_vacant')}/{r.get('total_vacant')} units ({r.get('long_vacant_pct')}%)")
    else:
        lines.append("\nVacancy Spikes: None")

    disc = data.get('discount_alerts', [])
    if disc:
        lines.append(f"\nHigh Discount Penetration (>40%): {len(disc)} sites")
        for r in disc:
            lines.append(f"  - {r.get('site_code')} ({r.get('name')}): {r.get('discount_penetration_pct')}% of occupied units discounted, avg {r.get('avg_discount_pct')}% off")
    else:
        lines.append("\nDiscount Alerts: None")

    return "\n".join(lines)


# =============================================================================
# Input schemas
# =============================================================================

_SCHEMA_OPTIONAL_SITE = {
    "type": "object",
    "properties": {
        "site_code": {
            "type": "string",
            "description": "Site code filter (e.g. 'L001'). If omitted, returns all sites / portfolio aggregate."
        },
    },
    "required": []
}

_SCHEMA_OPTIONAL_SITE_DATE = {
    "type": "object",
    "properties": {
        "site_code": {
            "type": "string",
            "description": "Site code filter (e.g. 'L001'). If omitted, returns all sites / portfolio aggregate."
        },
        "extract_date": {
            "type": "string",
            "description": "Snapshot date (YYYY-MM-DD). Defaults to latest available."
        },
    },
    "required": []
}


# =============================================================================
# Tool registration
# =============================================================================

def register_revenue_tools(server: Server, app: 'MCPServerApp') -> None:
    """Register revenue management tools with the MCP server."""

    if not hasattr(server, '_tool_handlers'):
        server._tool_handlers = {}

    from mcp_esa.services import revenue_service as svc

    # ==========================================
    # RM_get_portfolio_snapshot
    # ==========================================
    async def RM_get_portfolio_snapshot(
        auth_context: Optional[Dict] = None,
        extract_date: str = None,
    ) -> str:
        """Get current portfolio-wide KPIs: occupancy, revenue, REVPAS, penetration metrics."""
        try:
            snap = await svc.get_portfolio_snapshot(extract_date=extract_date)
            return _format_portfolio_snapshot(snap)
        except Exception as e:
            logger.error(f"RM_get_portfolio_snapshot error: {e}")
            return "Failed to retrieve portfolio snapshot. Check server logs."

    RM_get_portfolio_snapshot._input_schema = {
        "type": "object",
        "properties": {
            "extract_date": {
                "type": "string",
                "description": "Snapshot date (YYYY-MM-DD). Defaults to latest available."
            }
        },
        "required": []
    }
    server._tool_handlers["RM_get_portfolio_snapshot"] = RM_get_portfolio_snapshot

    # ==========================================
    # RM_get_site_performance
    # ==========================================
    async def RM_get_site_performance(
        auth_context: Optional[Dict] = None,
        site_code: str = None,
        extract_date: str = None,
    ) -> str:
        """Get per-site performance breakdown: occupancy, revenue, REVPAS, rates."""
        try:
            sites = await svc.get_site_performance(extract_date=extract_date, site_code=site_code)
            return _format_site_table(sites)
        except Exception as e:
            logger.error(f"RM_get_site_performance error: {e}")
            return "Failed to retrieve site performance. Check server logs."

    RM_get_site_performance._input_schema = _SCHEMA_OPTIONAL_SITE_DATE
    server._tool_handlers["RM_get_site_performance"] = RM_get_site_performance

    # ==========================================
    # RM_get_budget_variance
    # ==========================================
    async def RM_get_budget_variance(
        auth_context: Optional[Dict] = None,
        month: str = None,
        site_code: str = None,
    ) -> str:
        """Compare actual revenue and occupancy vs budget targets with variance and achievement percentages."""
        try:
            rows = await svc.get_budget_variance(month=month, site_code=site_code)
            return _format_budget_table(rows)
        except Exception as e:
            logger.error(f"RM_get_budget_variance error: {e}")
            return "Failed to retrieve budget variance. Check server logs."

    RM_get_budget_variance._input_schema = {
        "type": "object",
        "properties": {
            "month": {
                "type": "string",
                "description": "Target month (YYYY-MM-01). Defaults to current month."
            },
            "site_code": {
                "type": "string",
                "description": "Site code filter (e.g. 'L001'). If omitted, returns all sites."
            },
        },
        "required": []
    }
    server._tool_handlers["RM_get_budget_variance"] = RM_get_budget_variance

    # ==========================================
    # RM_get_occupancy_trends
    # ==========================================
    async def RM_get_occupancy_trends(
        auth_context: Optional[Dict] = None,
        days: int = 90,
        site_code: str = None,
    ) -> str:
        """Get daily occupancy time series (unit, area, economic) for the last N days."""
        try:
            days = min(int(days), 365)
            trends = await svc.get_occupancy_trends(days=days, site_code=site_code)
            if not trends:
                return "No occupancy trend data available."
            lines = [f"OCCUPANCY TRENDS (last {days} days, {len(trends)} data points):"]
            lines.append(f"{'Date':<12} {'UnitOcc%':>9} {'AreaOcc%':>9} {'EconOcc%':>9} {'Revenue':>14}")
            lines.append("-" * 57)
            for t in trends:
                lines.append(
                    f"{(t.get('date') or ''):<12} "
                    f"{_fmt_pct(t.get('unit_occ')):>9} "
                    f"{_fmt_pct(t.get('area_occ')):>9} "
                    f"{_fmt_pct(t.get('economic_occ')):>9} "
                    f"{_fmt_money(t.get('actual_revenue')):>14}"
                )
            return "\n".join(lines)
        except Exception as e:
            logger.error(f"RM_get_occupancy_trends error: {e}")
            return "Failed to retrieve occupancy trends. Check server logs."

    RM_get_occupancy_trends._input_schema = {
        "type": "object",
        "properties": {
            "days": {
                "type": "integer",
                "description": "Number of days to look back (max 365). Default: 90.",
                "default": 90
            },
            "site_code": {
                "type": "string",
                "description": "Site code filter. If omitted, returns portfolio-wide average."
            },
        },
        "required": []
    }
    server._tool_handlers["RM_get_occupancy_trends"] = RM_get_occupancy_trends

    # ==========================================
    # RM_get_movement_analysis
    # ==========================================
    async def RM_get_movement_analysis(
        auth_context: Optional[Dict] = None,
        days: int = 30,
        site_code: str = None,
    ) -> str:
        """Get move-in/move-out activity, net absorption, avg rates, and vacancy duration."""
        try:
            days = min(int(days), 365)
            data = await svc.get_movement_analysis(days=days, site_code=site_code)
            return _format_movement(data)
        except Exception as e:
            logger.error(f"RM_get_movement_analysis error: {e}")
            return "Failed to retrieve movement analysis. Check server logs."

    RM_get_movement_analysis._input_schema = {
        "type": "object",
        "properties": {
            "days": {
                "type": "integer",
                "description": "Number of days to analyze (max 365). Default: 30.",
                "default": 30
            },
            "site_code": {
                "type": "string",
                "description": "Site code filter. If omitted, returns portfolio-wide."
            },
        },
        "required": []
    }
    server._tool_handlers["RM_get_movement_analysis"] = RM_get_movement_analysis

    # ==========================================
    # RM_get_rate_analysis
    # ==========================================
    async def RM_get_rate_analysis(
        auth_context: Optional[Dict] = None,
        site_code: str = None,
        extract_date: str = None,
    ) -> str:
        """Rate benchmarking by unit category: in-place vs STD rates, discount %, vacancy days."""
        try:
            rows = await svc.get_rate_analysis(extract_date=extract_date, site_code=site_code)
            if not rows:
                return "No rate analysis data available."
            lines = ["RATE ANALYSIS BY CATEGORY:"]
            lines.append(f"{'Type':<8} {'Climate':<8} {'Size':<12} {'Occ':>5} {'Vac':>5} {'InPlace/Sqf':>12} {'STD/Sqf':>10} {'Disc%':>7} {'AvgVacDays':>11}")
            lines.append("-" * 82)
            for r in rows:
                lines.append(
                    f"{(r.get('label_type_code') or '-'):<8} "
                    f"{(r.get('label_climate_code') or '-'):<8} "
                    f"{(r.get('label_size_range') or '-'):<12} "
                    f"{_fmt_num(r.get('occ_count')):>5} "
                    f"{_fmt_num(r.get('vac_count')):>5} "
                    f"{_fmt_money(r.get('avg_inplace_sqft')):>12} "
                    f"{_fmt_money(r.get('avg_std_sqft')):>10} "
                    f"{_fmt_pct(r.get('avg_discount_pct')):>7} "
                    f"{_fmt_num(r.get('avg_days_vacant')):>11}"
                )
            return "\n".join(lines)
        except Exception as e:
            logger.error(f"RM_get_rate_analysis error: {e}")
            return "Failed to retrieve rate analysis. Check server logs."

    RM_get_rate_analysis._input_schema = _SCHEMA_OPTIONAL_SITE_DATE
    server._tool_handlers["RM_get_rate_analysis"] = RM_get_rate_analysis

    # ==========================================
    # RM_get_customer_segments
    # ==========================================
    async def RM_get_customer_segments(
        auth_context: Optional[Dict] = None,
        site_code: str = None,
        extract_date: str = None,
    ) -> str:
        """Tenant segmentation by length-of-stay: counts, rates, discount penetration per LOS bucket."""
        try:
            rows = await svc.get_customer_segments(extract_date=extract_date, site_code=site_code)
            if not rows:
                return "No customer segment data available."
            lines = ["CUSTOMER SEGMENTS BY LENGTH OF STAY:"]
            lines.append(f"{'LOS Range':<16} {'Tenants':>8} {'TotalRent':>14} {'AvgRate/Sqf':>12} {'Discounted':>11} {'AvgDisc%':>9}")
            lines.append("-" * 74)
            for r in rows:
                lines.append(
                    f"{(r.get('los_range') or '-'):<16} "
                    f"{_fmt_num(r.get('tenant_count')):>8} "
                    f"{_fmt_money(r.get('total_rent')):>14} "
                    f"{_fmt_money(r.get('avg_rate_sqft')):>12} "
                    f"{_fmt_num(r.get('discounted_count')):>11} "
                    f"{_fmt_pct(r.get('avg_discount_pct')):>9}"
                )
            return "\n".join(lines)
        except Exception as e:
            logger.error(f"RM_get_customer_segments error: {e}")
            return "Failed to retrieve customer segments. Check server logs."

    RM_get_customer_segments._input_schema = _SCHEMA_OPTIONAL_SITE_DATE
    server._tool_handlers["RM_get_customer_segments"] = RM_get_customer_segments

    # ==========================================
    # RM_analyze_revenue (LLM)
    # ==========================================
    async def RM_analyze_revenue(
        auth_context: Optional[Dict] = None,
        site_code: str = None,
        focus: str = "general",
    ) -> str:
        """
        AI-powered revenue analysis. Gathers portfolio data and uses LLM to provide insights.

        Focus areas: general, pricing, occupancy, demand, discounts
        """
        try:
            valid_focus = {"general", "pricing", "occupancy", "demand", "discounts"}
            focus = focus if focus in valid_focus else "general"

            snap, sites, segments, rates = await asyncio.gather(
                svc.get_portfolio_snapshot(),
                svc.get_site_performance(site_code=site_code),
                svc.get_customer_segments(site_code=site_code),
                svc.get_rate_analysis(site_code=site_code),
            )

            data_summary = _format_portfolio_snapshot(snap)
            data_summary += "\n\n" + _format_site_table(sites[:15])
            data_summary += "\n\nCUSTOMER SEGMENTS:\n" + json.dumps(segments[:10], indent=2, default=str)
            data_summary += "\n\nRATE ANALYSIS (top 20 categories):\n" + json.dumps(rates[:20], indent=2, default=str)

            from mcp_esa.services.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw data:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            focus_prompts = {
                "general": "Provide a comprehensive revenue health assessment covering occupancy, pricing, demand signals, and tenant mix.",
                "pricing": "Focus on pricing strategy: in-place rates vs standard rates, discount levels, rate optimization opportunities.",
                "occupancy": "Focus on occupancy patterns: site-level differences, vacancy concentration, absorption trends.",
                "demand": "Focus on demand signals: move-in/move-out trends, vacancy duration, market positioning.",
                "discounts": "Focus on discount analysis: penetration by site, discount depth, impact on revenue."
            }

            messages = [
                {
                    "role": "system",
                    "content": f"""You are a senior revenue management analyst for Extra Space Asia, a self-storage operator across Singapore, Korea, Malaysia, and Hong Kong.
Analyze the data and provide actionable insights. Be specific with numbers. Use bullet points.
{focus_prompts.get(focus, focus_prompts['general'])}

Structure your response as:
1. **Key Findings** (top 3-5 data-driven observations)
2. **Opportunities** (specific revenue optimization actions)
3. **Risks** (areas of concern with data evidence)
4. **Recommendations** (prioritized next steps)"""
                },
                {
                    "role": "user",
                    "content": f"Analyze this revenue data (focus: {focus}):\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=3000
            )

            scope = f"Site {site_code}" if site_code else "Portfolio"
            return f"Revenue Analysis — {scope} (Focus: {focus})\n{'=' * 50}\n\n{response['content']}"

        except Exception as e:
            logger.error(f"RM_analyze_revenue error: {e}", exc_info=True)
            return "Failed to generate revenue analysis. Check server logs."

    RM_analyze_revenue._input_schema = {
        "type": "object",
        "properties": {
            "site_code": {
                "type": "string",
                "description": "Site code filter (e.g. 'L001'). If omitted, analyzes full portfolio."
            },
            "focus": {
                "type": "string",
                "description": "Analysis focus area: general, pricing, occupancy, demand, discounts. Default: general.",
                "enum": ["general", "pricing", "occupancy", "demand", "discounts"],
                "default": "general"
            },
        },
        "required": []
    }
    server._tool_handlers["RM_analyze_revenue"] = RM_analyze_revenue

    # ==========================================
    # RM_detect_anomalies (LLM)
    # ==========================================
    async def RM_detect_anomalies(
        auth_context: Optional[Dict] = None,
        site_code: str = None,
    ) -> str:
        """AI-powered anomaly detection: flags occupancy drops, revenue shortfalls, vacancy spikes, and discount concerns."""
        try:
            anomalies = await svc.get_anomalies(site_code=site_code)
            data_summary = _format_anomalies(anomalies)

            total_alerts = sum(len(anomalies.get(k, [])) for k in ['occ_drops', 'revenue_concerns', 'vacancy_spikes', 'discount_alerts'])

            if total_alerts == 0:
                return f"Anomaly Detection — No Alerts\n{'=' * 50}\n\nNo anomalies detected across all monitored thresholds. Portfolio operating within normal parameters."

            from mcp_esa.services.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw alerts:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": """You are a revenue management watchdog for Extra Space Asia self-storage.
Review the anomaly alerts and provide:
1. **Severity Assessment** — rank each alert as Critical / Warning / Monitor
2. **Root Cause Hypotheses** — what might explain each anomaly (seasonal, competitive, operational)
3. **Recommended Actions** — specific steps for each alert, ordered by urgency
Be concise and actionable. Don't repeat raw numbers — interpret them."""
                },
                {
                    "role": "user",
                    "content": f"Review these anomaly alerts ({total_alerts} total):\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=2500
            )

            scope = f"Site {site_code}" if site_code else "Portfolio"
            return f"Anomaly Detection — {scope} ({total_alerts} alerts)\n{'=' * 50}\n\n{response['content']}"

        except Exception as e:
            logger.error(f"RM_detect_anomalies error: {e}", exc_info=True)
            return "Failed to run anomaly detection. Check server logs."

    RM_detect_anomalies._input_schema = _SCHEMA_OPTIONAL_SITE
    server._tool_handlers["RM_detect_anomalies"] = RM_detect_anomalies

    # ==========================================
    # RM_generate_executive_report (LLM)
    # ==========================================
    async def RM_generate_executive_report(
        auth_context: Optional[Dict] = None,
        site_code: str = None,
    ) -> str:
        """
        Generate a weekly executive briefing for leadership meetings.

        Covers portfolio health, budget tracking, demand signals, site winners/losers,
        anomalies, and recommended actions. Designed to be read in 3-5 minutes.
        """
        try:
            (
                snap_data, sites_data, budget_data,
                move_7d, move_30d, anomaly_data,
                trend_data, segment_data
            ) = await asyncio.gather(
                svc.get_portfolio_snapshot(),
                svc.get_site_performance(site_code=site_code),
                svc.get_budget_variance(site_code=site_code),
                svc.get_movement_analysis(days=7, site_code=site_code),
                svc.get_movement_analysis(days=30, site_code=site_code),
                svc.get_anomalies(site_code=site_code),
                svc.get_occupancy_trends(days=30, site_code=site_code),
                svc.get_customer_segments(site_code=site_code),
            )

            # Build comprehensive data summary for LLM
            data_parts = []
            data_parts.append(_format_portfolio_snapshot(snap_data))
            data_parts.append("\n" + _format_site_table(sites_data[:15]))
            data_parts.append("\n" + _format_budget_table(budget_data))

            data_parts.append(f"\nMOVEMENT (7-day): MI={move_7d.get('total_move_ins', 0)}, MO={move_7d.get('total_move_outs', 0)}, Net={move_7d.get('net_absorption', 0)}")
            data_parts.append(f"MOVEMENT (30-day): MI={move_30d.get('total_move_ins', 0)}, MO={move_30d.get('total_move_outs', 0)}, Net={move_30d.get('net_absorption', 0)}, Avg MI Rate/Sqf={_fmt_money(move_30d.get('avg_mi_rate_sqft'))}, Avg Days Vacant Before MI={_fmt_num(move_30d.get('avg_days_vacant_before_mi'))}")

            # Occupancy trend direction
            if trend_data and len(trend_data) >= 7:
                recent = trend_data[-7:]
                older = trend_data[-14:-7] if len(trend_data) >= 14 else trend_data[:7]
                avg_recent = sum(t.get('unit_occ', 0) or 0 for t in recent) / len(recent)
                avg_older = sum(t.get('unit_occ', 0) or 0 for t in older) / len(older)
                direction = "UP" if avg_recent > avg_older else "DOWN" if avg_recent < avg_older else "FLAT"
                data_parts.append(f"\nOCC TREND: {direction} (last 7d avg: {avg_recent:.2f}%, prior 7d: {avg_older:.2f}%)")

            # Top/bottom sites
            if len(sites_data) > 1:
                sorted_by_occ = sorted(sites_data, key=lambda s: s.get('occ_pct_unit') or 0, reverse=True)
                top3 = sorted_by_occ[:3]
                bottom3 = sorted_by_occ[-3:]
                data_parts.append("\nTOP 3 SITES (by Occ%):")
                for s in top3:
                    data_parts.append(f"  - {s.get('site_code')} ({s.get('name')}): {_fmt_pct(s.get('occ_pct_unit'))}, Rev {_fmt_money(s.get('revenue'))}")
                data_parts.append("BOTTOM 3 SITES (by Occ%):")
                for s in bottom3:
                    data_parts.append(f"  - {s.get('site_code')} ({s.get('name')}): {_fmt_pct(s.get('occ_pct_unit'))}, Rev {_fmt_money(s.get('revenue'))}")

            data_parts.append("\n" + _format_anomalies(anomaly_data))

            if segment_data:
                data_parts.append("\nTENANT MIX:")
                for seg in segment_data:
                    data_parts.append(f"  - {seg.get('los_range')}: {seg.get('tenant_count')} tenants, Avg {_fmt_money(seg.get('avg_rate_sqft'))}/sqf, {seg.get('discounted_count')} discounted")

            data_summary = "\n".join(data_parts)

            from mcp_esa.services.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw data for manual review:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": """You are the VP of Revenue Management at Extra Space Asia, preparing a weekly executive briefing for the leadership meeting.

Write a concise, data-driven report that a CEO can scan in 3-5 minutes. Use the exact numbers from the data. Do not invent numbers.

Structure the report EXACTLY as follows:

## Portfolio Health
2-3 bullet points on headline KPIs (occ%, revenue, REVPAS) with week-over-week and month-over-month direction.

## Budget Tracking
MTD achievement percentage. Which sites are ahead/behind budget. Projected month-end outlook.

## Movement & Demand
Net absorption velocity. Move-in rate trends. Vacancy duration signals. Compare 7-day vs 30-day activity to identify acceleration or deceleration.

## Winners & Losers
Top 3 performing sites and bottom 3 sites with brief context on why.

## Anomalies & Watch Items
Flag anything unusual — occupancy drops, revenue shortfalls, vacancy concentrations, discount creep. Severity: Critical / Warning / Monitor.

## Recommended Actions
3-5 specific, actionable items for the coming week. Each should reference a specific site or metric.

IMPORTANT: Be factual and grounded in the data. Flag genuine concerns but don't create alarm where data is stable. Use plain language — no jargon."""
                },
                {
                    "role": "user",
                    "content": f"Generate the weekly executive briefing from this data:\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=4000
            )

            from datetime import date
            scope = f" — {site_code}" if site_code else ""
            header = f"ESA Weekly Executive Briefing{scope}\nWeek of {date.today().isoformat()}\n{'=' * 50}"
            return f"{header}\n\n{response['content']}"

        except Exception as e:
            logger.error(f"RM_generate_executive_report error: {e}", exc_info=True)
            return "Failed to generate executive report. Check server logs."

    RM_generate_executive_report._input_schema = _SCHEMA_OPTIONAL_SITE
    server._tool_handlers["RM_generate_executive_report"] = RM_generate_executive_report

    # Log registration
    tool_names = [k for k in server._tool_handlers if k.startswith("RM_")]
    logger.info(f"Revenue Management tools registered: {', '.join(tool_names)} ({len(tool_names)} tools)")
