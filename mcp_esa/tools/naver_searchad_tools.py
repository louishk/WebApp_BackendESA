"""
Naver Search Ad Tools — MCP tool wrappers.

Mirrors the shape of google_ads_tools.py. All tools are prefixed `NSA_`.
"""

from __future__ import annotations

import json
import logging
from typing import Dict, List, Optional, TYPE_CHECKING

from mcp.server import Server

from mcp_esa.config.settings import get_settings
from mcp_esa.services.naver_searchad_service import (
    NaverSearchAdAPIError,
    NaverSearchAdConfig,
    NaverSearchAdService,
)

if TYPE_CHECKING:
    from mcp_esa.server.mcp_server import MCPServerApp

logger = logging.getLogger(__name__)


def _get_config() -> Optional[NaverSearchAdConfig]:
    s = get_settings()
    if not s.naver_searchad_enabled:
        return None
    if not (s.naver_searchad_api_key and s.naver_searchad_secret_key and s.naver_searchad_customer_id):
        return None
    return NaverSearchAdConfig(
        api_key=s.naver_searchad_api_key,
        secret_key=s.naver_searchad_secret_key,
        customer_id=s.naver_searchad_customer_id,
        base_url=s.naver_searchad_base_url,
    )


def _dump(obj) -> str:
    """Render API responses as pretty JSON, truncating very long arrays."""
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return str(obj)


def _err(e: Exception, op: str) -> str:
    if isinstance(e, NaverSearchAdAPIError):
        logger.warning("Naver %s failed: %s", op, e)
        return f"Naver Search Ad error ({op}): {e}"
    logger.error("Naver %s failed: %s", op, e, exc_info=True)
    return f"{op} failed. Check server logs for details."


def register_naver_searchad_tools(server: Server, app: "MCPServerApp") -> None:
    """Register all Naver Search Ad tools."""

    if not hasattr(server, "_tool_handlers"):
        server._tool_handlers = {}

    logger.info("Registering Naver Search Ad tools")

    # -----------------------------------------------------------------
    # Utility
    # -----------------------------------------------------------------
    async def nsa_test_connection(auth_context: Optional[Dict] = None) -> str:
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured. Set NAVER_SEARCHAD_* env vars or vault secrets."
        try:
            result = await NaverSearchAdService(config).test_connection()
            return (
                f"Naver Search Ad Connection: SUCCESS\n\n"
                f"Customer ID: {result['customer_id']}\n"
                f"Business Channels: {result['business_channel_count']}"
            )
        except Exception as e:
            return _err(e, "test_connection")

    async def nsa_list_business_channels(auth_context: Optional[Dict] = None) -> str:
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            channels = await NaverSearchAdService(config).list_business_channels()
            if not channels:
                return "No business channels found"
            out = [f"Found {len(channels)} business channel(s):\n"]
            for c in channels:
                out.append(
                    f"- {c.get('name', '(no name)')} "
                    f"[{c.get('channelTp', '')}] "
                    f"id={c.get('nccBusinessChannelId', '')} "
                    f"status={c.get('inspectStatus', '')}"
                )
            return "\n".join(out)
        except Exception as e:
            return _err(e, "list_business_channels")

    # -----------------------------------------------------------------
    # Campaigns
    # -----------------------------------------------------------------
    async def nsa_list_campaigns(
        auth_context: Optional[Dict] = None,
        ids: Optional[str] = None,
    ) -> str:
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            id_list = [i.strip() for i in ids.split(",")] if ids else None
            campaigns = await NaverSearchAdService(config).list_campaigns(id_list)
            if not campaigns:
                return "No campaigns found"
            out = [f"Found {len(campaigns)} campaign(s):\n"]
            for c in campaigns:
                out.append(
                    f"- {c.get('name')} "
                    f"id={c.get('nccCampaignId')} "
                    f"tp={c.get('campaignTp')} "
                    f"status={c.get('status')} "
                    f"dailyBudget={c.get('dailyBudget')}"
                )
            return "\n".join(out)
        except Exception as e:
            return _err(e, "list_campaigns")

    async def nsa_get_campaign(
        auth_context: Optional[Dict] = None,
        campaign_id: str = None,
    ) -> str:
        if not campaign_id:
            return "campaign_id is required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            data = await NaverSearchAdService(config).get_campaign(campaign_id)
            return _dump(data)
        except Exception as e:
            return _err(e, "get_campaign")

    async def nsa_create_campaign(
        auth_context: Optional[Dict] = None,
        name: str = None,
        campaign_tp: str = "WEB_SITE",
        daily_budget: int = 0,
        use_daily_budget: bool = False,
    ) -> str:
        if not name:
            return "name is required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            data = await NaverSearchAdService(config).create_campaign(
                name=name,
                campaign_tp=campaign_tp,
                daily_budget=daily_budget,
                use_daily_budget=use_daily_budget,
            )
            return f"Campaign created\n\n{_dump(data)}"
        except Exception as e:
            return _err(e, "create_campaign")

    async def nsa_set_campaign_status(
        auth_context: Optional[Dict] = None,
        campaign_id: str = None,
        paused: bool = True,
    ) -> str:
        if not campaign_id:
            return "campaign_id is required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            data = await NaverSearchAdService(config).set_campaign_status(
                campaign_id, paused
            )
            return f"Campaign {'paused' if paused else 'resumed'}\n\n{_dump(data)}"
        except Exception as e:
            return _err(e, "set_campaign_status")

    async def nsa_delete_campaign(
        auth_context: Optional[Dict] = None,
        campaign_id: str = None,
    ) -> str:
        if not campaign_id:
            return "campaign_id is required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            await NaverSearchAdService(config).delete_campaign(campaign_id)
            return f"Campaign {campaign_id} deleted"
        except Exception as e:
            return _err(e, "delete_campaign")

    # -----------------------------------------------------------------
    # Ad Groups / Keywords / Ads
    # -----------------------------------------------------------------
    async def nsa_list_ad_groups(
        auth_context: Optional[Dict] = None,
        campaign_id: Optional[str] = None,
        ids: Optional[str] = None,
    ) -> str:
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            id_list = [i.strip() for i in ids.split(",")] if ids else None
            groups = await NaverSearchAdService(config).list_ad_groups(
                campaign_id=campaign_id, ids=id_list
            )
            if not groups:
                return "No ad groups found"
            out = [f"Found {len(groups)} ad group(s):\n"]
            for g in groups:
                out.append(
                    f"- {g.get('name')} "
                    f"id={g.get('nccAdgroupId')} "
                    f"status={g.get('status')} "
                    f"bid={g.get('bidAmt')}"
                )
            return "\n".join(out)
        except Exception as e:
            return _err(e, "list_ad_groups")

    async def nsa_get_ad_group(
        auth_context: Optional[Dict] = None,
        ad_group_id: str = None,
    ) -> str:
        if not ad_group_id:
            return "ad_group_id is required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            return _dump(await NaverSearchAdService(config).get_ad_group(ad_group_id))
        except Exception as e:
            return _err(e, "get_ad_group")

    async def nsa_list_keywords(
        auth_context: Optional[Dict] = None,
        ad_group_id: str = None,
    ) -> str:
        if not ad_group_id:
            return "ad_group_id is required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            kws = await NaverSearchAdService(config).list_keywords(ad_group_id)
            if not kws:
                return f"No keywords found for ad group {ad_group_id}"
            out = [f"Found {len(kws)} keyword(s):\n"]
            for k in kws:
                out.append(
                    f"- {k.get('keyword')} "
                    f"id={k.get('nccKeywordId')} "
                    f"bid={k.get('bidAmt')} "
                    f"status={k.get('status')}"
                )
            return "\n".join(out)
        except Exception as e:
            return _err(e, "list_keywords")

    async def nsa_update_keyword_bid(
        auth_context: Optional[Dict] = None,
        keyword_id: str = None,
        bid_amt: int = None,
    ) -> str:
        if not keyword_id or bid_amt is None:
            return "keyword_id and bid_amt are required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            data = await NaverSearchAdService(config).update_keyword_bid(
                keyword_id, int(bid_amt)
            )
            return f"Keyword bid updated\n\n{_dump(data)}"
        except Exception as e:
            return _err(e, "update_keyword_bid")

    async def nsa_list_ads(
        auth_context: Optional[Dict] = None,
        ad_group_id: str = None,
    ) -> str:
        if not ad_group_id:
            return "ad_group_id is required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            ads = await NaverSearchAdService(config).list_ads(ad_group_id)
            if not ads:
                return f"No ads found for ad group {ad_group_id}"
            return _dump(ads)
        except Exception as e:
            return _err(e, "list_ads")

    # -----------------------------------------------------------------
    # Stats / Reports / Billing / Keyword tool
    # -----------------------------------------------------------------
    async def nsa_get_stats(
        auth_context: Optional[Dict] = None,
        ids: str = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        date_preset: Optional[str] = None,
        fields: Optional[str] = None,
    ) -> str:
        if not ids:
            return "ids (comma-separated entity IDs) is required"
        if not ((since and until) or date_preset):
            return "Provide either (since, until) or date_preset"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            id_list = [i.strip() for i in ids.split(",")]
            field_list = [f.strip() for f in fields.split(",")] if fields else None
            data = await NaverSearchAdService(config).get_stats(
                ids=id_list,
                fields=field_list,
                since=since,
                until=until,
                date_preset=date_preset,
            )
            return _dump(data)
        except Exception as e:
            return _err(e, "get_stats")

    async def nsa_create_stat_report(
        auth_context: Optional[Dict] = None,
        report_tp: str = None,
        stat_dt: str = None,
    ) -> str:
        if not report_tp or not stat_dt:
            return "report_tp and stat_dt (YYYY-MM-DD) are required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            data = await NaverSearchAdService(config).create_stat_report(report_tp, stat_dt)
            return _dump(data)
        except Exception as e:
            return _err(e, "create_stat_report")

    async def nsa_get_stat_report(
        auth_context: Optional[Dict] = None,
        report_job_id: str = None,
    ) -> str:
        if not report_job_id:
            return "report_job_id is required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            return _dump(await NaverSearchAdService(config).get_stat_report(report_job_id))
        except Exception as e:
            return _err(e, "get_stat_report")

    async def nsa_keyword_tool(
        auth_context: Optional[Dict] = None,
        hint_keywords: Optional[str] = None,
        show_detail: bool = True,
    ) -> str:
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            kws = [k.strip() for k in hint_keywords.split(",")] if hint_keywords else None
            return _dump(
                await NaverSearchAdService(config).keyword_tool(
                    hint_keywords=kws, show_detail=show_detail
                )
            )
        except Exception as e:
            return _err(e, "keyword_tool")

    async def nsa_get_bizmoney_balance(auth_context: Optional[Dict] = None) -> str:
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            return _dump(await NaverSearchAdService(config).get_bizmoney_balance())
        except Exception as e:
            return _err(e, "get_bizmoney_balance")

    async def nsa_get_bizmoney_cost(
        auth_context: Optional[Dict] = None,
        start_date: str = None,
        end_date: str = None,
    ) -> str:
        if not (start_date and end_date):
            return "start_date and end_date (YYYY-MM-DD) are required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            return _dump(await NaverSearchAdService(config).get_bizmoney_cost(start_date, end_date))
        except Exception as e:
            return _err(e, "get_bizmoney_cost")

    # =================================================================
    # AI-POWERED ANALYSIS TOOLS
    # =================================================================

    async def _fetch_snapshot(
        svc: NaverSearchAdService,
        date_preset: str,
        include_keywords_for_top: int = 0,
    ) -> Dict:
        """Fetch a campaign+adgroup snapshot with stats. Used by AI tools.

        If include_keywords_for_top > 0, also pulls keywords + stats for the
        top-N ad groups by recent spend.
        """
        import asyncio

        campaigns = await svc.list_campaigns()
        ad_groups = await svc.list_ad_groups()

        async def stats_for(ids: List[str]) -> Dict[str, Dict]:
            if not ids:
                return {}
            # Naver /stats caps work best < ~100 ids; chunk
            by_id: Dict[str, Dict] = {}
            for i in range(0, len(ids), 80):
                chunk = ids[i : i + 80]
                try:
                    raw = await svc.get_stats(ids=chunk, date_preset=date_preset)
                except NaverSearchAdAPIError as e:
                    logger.warning("stats batch failed: %s", e)
                    continue
                # Response shape: {"data": [{"id":..., "impCnt":...}, ...]}
                rows = raw.get("data", []) if isinstance(raw, dict) else raw or []
                for row in rows:
                    rid = row.get("id") or row.get("nccId")
                    if rid:
                        by_id[str(rid)] = row
            return by_id

        campaign_ids = [c.get("nccCampaignId") for c in campaigns if c.get("nccCampaignId")]
        adgroup_ids = [g.get("nccAdgroupId") for g in ad_groups if g.get("nccAdgroupId")]

        campaign_stats, adgroup_stats = await asyncio.gather(
            stats_for(campaign_ids),
            stats_for(adgroup_ids),
        )

        keyword_bundles: List[Dict] = []
        if include_keywords_for_top > 0 and ad_groups:
            ranked = sorted(
                ad_groups,
                key=lambda g: adgroup_stats.get(g.get("nccAdgroupId", ""), {}).get("salesAmt", 0) or 0,
                reverse=True,
            )[:include_keywords_for_top]
            for g in ranked:
                gid = g.get("nccAdgroupId")
                try:
                    kws = await svc.list_keywords(gid)
                except NaverSearchAdAPIError:
                    continue
                if not kws:
                    continue
                kw_ids = [k.get("nccKeywordId") for k in kws if k.get("nccKeywordId")]
                kw_stats = await stats_for(kw_ids)
                for k in kws:
                    kid = k.get("nccKeywordId")
                    keyword_bundles.append(
                        {
                            "ad_group_id": gid,
                            "ad_group_name": g.get("name"),
                            "keyword_id": kid,
                            "keyword": k.get("keyword"),
                            "bidAmt": k.get("bidAmt"),
                            "status": k.get("status"),
                            "metrics": kw_stats.get(kid, {}),
                        }
                    )

        return {
            "campaigns": [
                {**c, "metrics": campaign_stats.get(c.get("nccCampaignId", ""), {})}
                for c in campaigns
            ],
            "ad_groups": [
                {**g, "metrics": adgroup_stats.get(g.get("nccAdgroupId", ""), {})}
                for g in ad_groups
            ],
            "keywords": keyword_bundles,
        }

    def _summarize_campaigns(snapshot: Dict) -> str:
        lines = []
        total_imp = total_clk = total_cost = total_conv = 0
        for c in snapshot["campaigns"]:
            m = c.get("metrics") or {}
            imp = int(m.get("impCnt", 0) or 0)
            clk = int(m.get("clkCnt", 0) or 0)
            cost = int(m.get("salesAmt", 0) or 0)
            conv = int(m.get("ccnt", 0) or 0)
            total_imp += imp; total_clk += clk; total_cost += cost; total_conv += conv
            lines.append(
                f"- {c.get('name')} [{c.get('campaignTp')}/{c.get('status')}] "
                f"dailyBudget=₩{c.get('dailyBudget', 0):,} "
                f"impCnt={imp:,} clkCnt={clk:,} spend=₩{cost:,} ccnt={conv}"
            )
        header = (
            f"ACCOUNT TOTALS: impCnt={total_imp:,} clkCnt={total_clk:,} "
            f"spend=₩{total_cost:,} ccnt={total_conv}\n\nCAMPAIGNS ({len(snapshot['campaigns'])}):\n"
        )
        return header + "\n".join(lines)

    def _summarize_keywords(snapshot: Dict, limit: int = 60) -> str:
        rows = []
        for k in snapshot["keywords"][:limit]:
            m = k.get("metrics") or {}
            rows.append(
                f"- [{k.get('ad_group_name')}] {k.get('keyword')} "
                f"bid=₩{k.get('bidAmt', 0):,} "
                f"impCnt={int(m.get('impCnt', 0) or 0):,} "
                f"clkCnt={int(m.get('clkCnt', 0) or 0):,} "
                f"spend=₩{int(m.get('salesAmt', 0) or 0):,} "
                f"ccnt={int(m.get('ccnt', 0) or 0)} "
                f"avgRnk={float(m.get('avgRnk', 0) or 0):.1f}"
            )
        return f"\nKEYWORDS ({len(snapshot['keywords'])} total, showing {min(limit, len(snapshot['keywords']))}):\n" + "\n".join(rows)

    async def _llm_chat(system: str, user: str, max_tokens: int = 2500) -> str:
        """Call the configured chat provider. Returns None if unavailable."""
        from mcp_esa.services.llm_manager import get_llm_manager

        mgr = get_llm_manager()
        if not mgr:
            return None
        try:
            provider = mgr.get_chat_provider()
        except ValueError:
            return None
        resp = await provider.chat_completion(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.3,
            max_tokens=max_tokens,
        )
        return resp.get("content") if isinstance(resp, dict) else str(resp)

    async def nsa_audit_account(
        auth_context: Optional[Dict] = None,
        date_preset: str = "last30days",
    ) -> str:
        """AI-powered audit of the Naver Search Ad account."""
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            svc = NaverSearchAdService(config)
            snap = await _fetch_snapshot(svc, date_preset, include_keywords_for_top=3)
            data = _summarize_campaigns(snap) + "\n" + _summarize_keywords(snap, limit=40)

            system = (
                "You are an expert Naver Search Ad (검색광고) auditor. Naver is the dominant "
                "Korean search engine and its ad platform is analogous to Google Ads but with "
                "KRW-denominated costs and HMAC-signed APIs. Produce a structured audit with:\n\n"
                "1. **Executive Summary** (2-3 sentences on overall health)\n"
                "2. **Performance Scorecard** (1-10): Campaign Structure, Keyword Health, "
                "Budget Efficiency, Conversion Performance\n"
                "3. **Key Findings** (top 5 with numbers)\n"
                "4. **Prioritized Recommendations** (High/Medium/Low impact, concrete actions)\n"
                "5. **Quick Wins**\n\n"
                "Use KRW (₩) for currency. Naver has no Quality Score, so use avgRnk and CTR "
                "as proxies for keyword health."
            )
            content = await _llm_chat(system, f"Audit this Naver account:\n\n{data}", 3000)
            if not content:
                return f"LLM unavailable. Raw snapshot:\n\n{data}"
            return f"Naver Search Ad Account Audit ({date_preset})\n{'=' * 50}\n\n{content}"
        except Exception as e:
            return _err(e, "audit_account")

    async def nsa_analyze_keywords(
        auth_context: Optional[Dict] = None,
        ad_group_id: Optional[str] = None,
        date_preset: str = "last30days",
    ) -> str:
        """AI keyword strategy analysis. If ad_group_id omitted, analyzes top 5 ad groups."""
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            svc = NaverSearchAdService(config)
            if ad_group_id:
                # Focused analysis on one ad group
                kws = await svc.list_keywords(ad_group_id)
                kw_ids = [k.get("nccKeywordId") for k in kws if k.get("nccKeywordId")]
                stats_raw = await svc.get_stats(ids=kw_ids, date_preset=date_preset) if kw_ids else {}
                by_id: Dict[str, Dict] = {}
                rows = stats_raw.get("data", []) if isinstance(stats_raw, dict) else stats_raw or []
                for r in rows:
                    rid = r.get("id") or r.get("nccId")
                    if rid:
                        by_id[str(rid)] = r
                lines = [f"AD GROUP {ad_group_id} — keywords ({len(kws)}):\n"]
                for k in kws[:80]:
                    m = by_id.get(k.get("nccKeywordId", ""), {})
                    lines.append(
                        f"- {k.get('keyword')} bid=₩{k.get('bidAmt', 0):,} "
                        f"status={k.get('status')} "
                        f"impCnt={int(m.get('impCnt', 0) or 0):,} "
                        f"clkCnt={int(m.get('clkCnt', 0) or 0):,} "
                        f"spend=₩{int(m.get('salesAmt', 0) or 0):,} "
                        f"ccnt={int(m.get('ccnt', 0) or 0)} "
                        f"avgRnk={float(m.get('avgRnk', 0) or 0):.1f}"
                    )
                data = "\n".join(lines)
            else:
                snap = await _fetch_snapshot(svc, date_preset, include_keywords_for_top=5)
                data = _summarize_keywords(snap, limit=120)

            system = (
                "You are a Naver Search Ad keyword strategist. Provide:\n"
                "1. **Top Performers** (scale with higher bids)\n"
                "2. **Underperformers** (pause / reduce bid, with reasons)\n"
                "3. **Bid Optimization** (specific KRW bid suggestions)\n"
                "4. **Match / Coverage Gaps**\n"
                "5. **Keywords to Pause Outright**\n\n"
                "Use KRW. Naver has no Quality Score — use avgRnk and CTR."
            )
            content = await _llm_chat(system, f"Analyze these Naver keywords:\n\n{data}", 2500)
            if not content:
                return f"LLM unavailable. Raw data:\n\n{data}"
            return f"Keyword Strategy Analysis ({date_preset})\n{'=' * 50}\n\n{content}"
        except Exception as e:
            return _err(e, "analyze_keywords")

    async def nsa_analyze_trends(
        auth_context: Optional[Dict] = None,
        since: str = None,
        until: str = None,
        breakdown: str = "dayOfWeek",
    ) -> str:
        """AI trend analysis across a date range, broken down by day/hour/device."""
        if not (since and until):
            return "since and until (YYYY-MM-DD) are required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            svc = NaverSearchAdService(config)
            campaigns = await svc.list_campaigns()
            ids = [c.get("nccCampaignId") for c in campaigns if c.get("nccCampaignId")]
            if not ids:
                return "No campaigns found"
            # Fetch stats with breakdown over the explicit range
            import json as _json
            params = {
                "ids": ids,
                "fields": _json.dumps(["impCnt", "clkCnt", "salesAmt", "ctr", "cpc", "ccnt"]),
                "timeRange": _json.dumps({"since": since, "until": until}),
                "breakdown": breakdown,
            }
            raw = await svc._request("GET", "/stats", params=params)
            data = _dump(raw)[:8000]

            system = (
                "You are a Naver Search Ad trend analyst. Given a stat series broken down by "
                f"'{breakdown}' across {since}→{until}, identify:\n"
                "1. Strong vs weak time windows (with numbers)\n"
                "2. Spend efficiency patterns\n"
                "3. Day-parting / scheduling recommendations\n"
                "4. Seasonal or step changes to investigate\n\n"
                "Use KRW for money. Be specific."
            )
            content = await _llm_chat(system, f"Analyze these Naver trends:\n\n{data}", 4000)
            if not content:
                return f"LLM unavailable. Raw data:\n\n{data}"
            return f"Trend Analysis {since}→{until} [{breakdown}]\n{'=' * 50}\n\n{content}"
        except Exception as e:
            return _err(e, "analyze_trends")

    async def nsa_suggest_negative_keywords(
        auth_context: Optional[Dict] = None,
        ad_group_id: Optional[str] = None,
        date_preset: str = "last30days",
    ) -> str:
        """Identify wasteful keywords / terms to consider as negatives.

        Naver has no search-terms report; this analyzes registered keywords with
        spend-but-no-conversions, which are the closest analog.
        """
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            svc = NaverSearchAdService(config)
            snap = await _fetch_snapshot(svc, date_preset, include_keywords_for_top=5)
            waste = [
                k for k in snap["keywords"]
                if int((k.get("metrics") or {}).get("salesAmt", 0) or 0) > 0
                and int((k.get("metrics") or {}).get("ccnt", 0) or 0) == 0
            ]
            waste.sort(
                key=lambda k: int((k.get("metrics") or {}).get("salesAmt", 0) or 0),
                reverse=True,
            )
            if ad_group_id:
                waste = [k for k in waste if k.get("ad_group_id") == ad_group_id]

            lines = [f"WASTEFUL KEYWORDS (spend > 0, conversions = 0) — {len(waste)}:\n"]
            for k in waste[:50]:
                m = k.get("metrics") or {}
                lines.append(
                    f"- [{k.get('ad_group_name')}] {k.get('keyword')}: "
                    f"spend=₩{int(m.get('salesAmt', 0) or 0):,} "
                    f"clkCnt={int(m.get('clkCnt', 0) or 0)} "
                    f"impCnt={int(m.get('impCnt', 0) or 0):,}"
                )
            data = "\n".join(lines)

            system = (
                "You are a Naver Search Ad optimization expert. Given wasteful keywords "
                "(with spend but no conversions), recommend:\n"
                "1. Keywords to pause immediately\n"
                "2. Keywords to reduce bids on (suggest % reduction)\n"
                "3. Candidate negative keywords / phrases\n"
                "4. Root-cause hypotheses (landing page, intent mismatch, etc.)\n\n"
                "Use KRW. Be specific."
            )
            content = await _llm_chat(system, f"Review waste:\n\n{data}", 4000)
            if not content:
                return f"LLM unavailable. Raw data:\n\n{data}"
            return f"Negative Keyword Suggestions ({date_preset})\n{'=' * 50}\n\n{content}"
        except Exception as e:
            return _err(e, "suggest_negative_keywords")

    async def nsa_optimize_budget(
        auth_context: Optional[Dict] = None,
        date_preset: str = "last30days",
    ) -> str:
        """AI-powered budget reallocation across campaigns."""
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            svc = NaverSearchAdService(config)
            snap = await _fetch_snapshot(svc, date_preset)
            data = _summarize_campaigns(snap)

            system = (
                "You are a Naver Search Ad budget optimization expert. For each campaign, "
                "compare dailyBudget vs actual spend, clicks, and conversions. Recommend:\n"
                "1. Campaigns to **increase** dailyBudget (with new KRW amount + justification)\n"
                "2. Campaigns to **decrease** or pause\n"
                "3. A total budget reallocation plan (net change = 0 KRW)\n"
                "4. Risks and things to monitor after the change\n\n"
                "Use KRW. Be concrete."
            )
            content = await _llm_chat(system, f"Optimize budgets:\n\n{data}", 6000)
            if not content:
                return f"LLM unavailable. Raw data:\n\n{data}"
            return f"Budget Optimization ({date_preset})\n{'=' * 50}\n\n{content}"
        except Exception as e:
            return _err(e, "optimize_budget")

    async def nsa_generate_report(
        auth_context: Optional[Dict] = None,
        date_preset: str = "last30days",
        report_type: str = "executive",
    ) -> str:
        """Generate an executive / detailed / optimization report.

        report_type: executive | detailed | optimization
        """
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            svc = NaverSearchAdService(config)
            snap = await _fetch_snapshot(svc, date_preset, include_keywords_for_top=3)
            data = _summarize_campaigns(snap) + "\n" + _summarize_keywords(snap, limit=40)

            templates = {
                "executive": (
                    "Write a one-page executive report for leadership. Structure:\n"
                    "- Headline metrics (spend, clicks, conversions, ROAS if derivable)\n"
                    "- Wins and concerns\n"
                    "- 3 strategic recommendations\n"
                    "Tone: concise, non-technical, KRW."
                ),
                "detailed": (
                    "Write a detailed performance report covering every campaign and the top "
                    "keywords, with period-over-period qualitative assessment, and "
                    "prioritized action items. Use KRW."
                ),
                "optimization": (
                    "Write an optimization playbook: the 10 highest-impact changes ranked by "
                    "estimated KRW impact, each with a step-by-step action plan."
                ),
            }
            system = (
                "You are a Naver Search Ad reporting specialist. "
                + templates.get(report_type, templates["executive"])
            )
            content = await _llm_chat(system, f"Account data:\n\n{data}", 3000)
            if not content:
                return f"LLM unavailable. Raw data:\n\n{data}"
            return f"Naver Search Ad {report_type.title()} Report ({date_preset})\n{'=' * 50}\n\n{content}"
        except Exception as e:
            return _err(e, "generate_report")

    # -----------------------------------------------------------------
    # Input schemas
    # -----------------------------------------------------------------
    _no_params = {"type": "object", "properties": {}}

    nsa_test_connection._input_schema = _no_params
    nsa_list_business_channels._input_schema = _no_params
    nsa_get_bizmoney_balance._input_schema = _no_params

    nsa_list_campaigns._input_schema = {
        "type": "object",
        "properties": {
            "ids": {"type": "string", "description": "Optional comma-separated nccCampaignIds"},
        },
    }
    nsa_get_campaign._input_schema = {
        "type": "object",
        "properties": {"campaign_id": {"type": "string"}},
        "required": ["campaign_id"],
    }
    nsa_create_campaign._input_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "campaign_tp": {
                "type": "string",
                "description": "WEB_SITE | SHOPPING | POWER_CONTENTS | BRAND_SEARCH | PLACE",
                "default": "WEB_SITE",
            },
            "daily_budget": {"type": "integer", "description": "KRW; 0 = unlimited", "default": 0},
            "use_daily_budget": {"type": "boolean", "default": False},
        },
        "required": ["name"],
    }
    nsa_set_campaign_status._input_schema = {
        "type": "object",
        "properties": {
            "campaign_id": {"type": "string"},
            "paused": {"type": "boolean", "default": True},
        },
        "required": ["campaign_id"],
    }
    nsa_delete_campaign._input_schema = {
        "type": "object",
        "properties": {"campaign_id": {"type": "string"}},
        "required": ["campaign_id"],
    }

    nsa_list_ad_groups._input_schema = {
        "type": "object",
        "properties": {
            "campaign_id": {"type": "string", "description": "Filter by nccCampaignId"},
            "ids": {"type": "string", "description": "Comma-separated nccAdgroupIds"},
        },
    }
    nsa_get_ad_group._input_schema = {
        "type": "object",
        "properties": {"ad_group_id": {"type": "string"}},
        "required": ["ad_group_id"],
    }
    nsa_list_keywords._input_schema = {
        "type": "object",
        "properties": {"ad_group_id": {"type": "string"}},
        "required": ["ad_group_id"],
    }
    nsa_update_keyword_bid._input_schema = {
        "type": "object",
        "properties": {
            "keyword_id": {"type": "string"},
            "bid_amt": {"type": "integer", "description": "Bid in KRW"},
        },
        "required": ["keyword_id", "bid_amt"],
    }
    nsa_list_ads._input_schema = {
        "type": "object",
        "properties": {"ad_group_id": {"type": "string"}},
        "required": ["ad_group_id"],
    }

    nsa_get_stats._input_schema = {
        "type": "object",
        "properties": {
            "ids": {"type": "string", "description": "Comma-separated entity IDs (campaign/adgroup/keyword/ad)"},
            "since": {"type": "string", "description": "YYYY-MM-DD (with `until`)"},
            "until": {"type": "string", "description": "YYYY-MM-DD (with `since`)"},
            "date_preset": {"type": "string", "description": "today | yesterday | last7days | ..."},
            "fields": {"type": "string", "description": "Comma-separated metric fields (default impCnt,clkCnt,salesAmt,ctr,cpc,avgRnk,ccnt)"},
        },
        "required": ["ids"],
    }
    nsa_create_stat_report._input_schema = {
        "type": "object",
        "properties": {
            "report_tp": {"type": "string", "description": "AD | AD_DETAIL (only these two are accepted by /stat-reports)"},
            "stat_dt": {"type": "string", "description": "YYYY-MM-DD (normalized to ISO datetime internally)"},
        },
        "required": ["report_tp", "stat_dt"],
    }
    nsa_get_stat_report._input_schema = {
        "type": "object",
        "properties": {"report_job_id": {"type": "string"}},
        "required": ["report_job_id"],
    }

    nsa_keyword_tool._input_schema = {
        "type": "object",
        "properties": {
            "hint_keywords": {"type": "string", "description": "Comma-separated seed keywords"},
            "show_detail": {"type": "boolean", "default": True},
        },
    }
    nsa_get_bizmoney_cost._input_schema = {
        "type": "object",
        "properties": {
            "start_date": {"type": "string", "description": "YYYY-MM-DD"},
            "end_date": {"type": "string", "description": "YYYY-MM-DD"},
        },
        "required": ["start_date", "end_date"],
    }

    # AI analysis schemas
    _date_preset_only = {
        "type": "object",
        "properties": {
            "date_preset": {
                "type": "string",
                "description": "today | yesterday | last7days | last14days | last30days",
                "default": "last30days",
            },
        },
    }
    nsa_audit_account._input_schema = _date_preset_only
    nsa_optimize_budget._input_schema = _date_preset_only
    nsa_analyze_keywords._input_schema = {
        "type": "object",
        "properties": {
            "ad_group_id": {
                "type": "string",
                "description": "Optional — focus on one ad group. Omit for top-5 analysis.",
            },
            "date_preset": {"type": "string", "default": "last30days"},
        },
    }
    nsa_analyze_trends._input_schema = {
        "type": "object",
        "properties": {
            "since": {"type": "string", "description": "YYYY-MM-DD"},
            "until": {"type": "string", "description": "YYYY-MM-DD"},
            "breakdown": {
                "type": "string",
                "description": "pcMobile | hh24 | dayOfWeek",
                "default": "dayOfWeek",
            },
        },
        "required": ["since", "until"],
    }
    nsa_suggest_negative_keywords._input_schema = {
        "type": "object",
        "properties": {
            "ad_group_id": {"type": "string", "description": "Optional filter"},
            "date_preset": {"type": "string", "default": "last30days"},
        },
    }
    nsa_generate_report._input_schema = {
        "type": "object",
        "properties": {
            "date_preset": {"type": "string", "default": "last30days"},
            "report_type": {
                "type": "string",
                "description": "executive | detailed | optimization",
                "default": "executive",
            },
        },
    }

    # -----------------------------------------------------------------
    # Register
    # -----------------------------------------------------------------
    tools = {
        "NSA_test_connection": nsa_test_connection,
        "NSA_list_business_channels": nsa_list_business_channels,
        "NSA_list_campaigns": nsa_list_campaigns,
        "NSA_get_campaign": nsa_get_campaign,
        "NSA_create_campaign": nsa_create_campaign,
        "NSA_set_campaign_status": nsa_set_campaign_status,
        "NSA_delete_campaign": nsa_delete_campaign,
        "NSA_list_ad_groups": nsa_list_ad_groups,
        "NSA_get_ad_group": nsa_get_ad_group,
        "NSA_list_keywords": nsa_list_keywords,
        "NSA_update_keyword_bid": nsa_update_keyword_bid,
        "NSA_list_ads": nsa_list_ads,
        "NSA_get_stats": nsa_get_stats,
        "NSA_create_stat_report": nsa_create_stat_report,
        "NSA_get_stat_report": nsa_get_stat_report,
        "NSA_keyword_tool": nsa_keyword_tool,
        "NSA_get_bizmoney_balance": nsa_get_bizmoney_balance,
        "NSA_get_bizmoney_cost": nsa_get_bizmoney_cost,
        # AI-powered analysis
        "NSA_audit_account": nsa_audit_account,
        "NSA_analyze_keywords": nsa_analyze_keywords,
        "NSA_analyze_trends": nsa_analyze_trends,
        "NSA_suggest_negative_keywords": nsa_suggest_negative_keywords,
        "NSA_optimize_budget": nsa_optimize_budget,
        "NSA_generate_report": nsa_generate_report,
    }

    for name, handler in tools.items():
        server._tool_handlers[name] = handler

    logger.info(f"Registered {len(tools)} Naver Search Ad tools")
