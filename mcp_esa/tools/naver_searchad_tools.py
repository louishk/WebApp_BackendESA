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
        date: str = None,
    ) -> str:
        if not date:
            return "date (YYYY-MM-DD) is required"
        config = _get_config()
        if not config:
            return "Naver Search Ad not configured"
        try:
            return _dump(await NaverSearchAdService(config).get_bizmoney_cost(date))
        except Exception as e:
            return _err(e, "get_bizmoney_cost")

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
            "report_tp": {"type": "string", "description": "AD | AD_DETAIL | AD_CONVERSION | ADGROUP | CAMPAIGN | KEYWORD | BUSINESS_CHANNEL"},
            "stat_dt": {"type": "string", "description": "YYYY-MM-DD"},
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
        "properties": {"date": {"type": "string", "description": "YYYY-MM-DD"}},
        "required": ["date"],
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
    }

    for name, handler in tools.items():
        server._tool_handlers[name] = handler

    logger.info(f"Registered {len(tools)} Naver Search Ad tools")
