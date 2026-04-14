"""
Naver Search Ad Service

Thin async wrapper around the Naver Search Ad REST API.
Docs: https://naver.github.io/searchad-apidoc/
Auth: HMAC-SHA256 request signing (see Authentication/README.md).

All requests carry four headers:
    X-Timestamp, X-API-KEY, X-Customer, X-Signature
The signature is base64(HMAC-SHA256(secret, "{ts}.{METHOD}.{path}")) — path only,
no host and no query string.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass
class NaverSearchAdConfig:
    api_key: str
    secret_key: str
    customer_id: str
    base_url: str = "https://api.searchad.naver.com"
    timeout: int = 30


class NaverSearchAdAPIError(Exception):
    def __init__(self, message: str, code: Optional[int] = None, status: Optional[int] = None):
        super().__init__(message)
        self.code = code
        self.status = status


class NaverSearchAdService:
    """Async client for Naver Search Ad."""

    def __init__(self, config: NaverSearchAdConfig):
        if not (config.api_key and config.secret_key and config.customer_id):
            raise ValueError("Naver Search Ad: api_key, secret_key, customer_id all required")
        self.config = config

    # ------------------------------------------------------------------ auth
    def _sign(self, timestamp: str, method: str, path: str) -> str:
        message = f"{timestamp}.{method}.{path}"
        digest = hmac.new(
            self.config.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _headers(self, method: str, path: str) -> Dict[str, str]:
        ts = str(round(time.time() * 1000))
        return {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Timestamp": ts,
            "X-API-KEY": self.config.api_key,
            "X-Customer": str(self.config.customer_id),
            "X-Signature": self._sign(ts, method.upper(), path),
        }

    # --------------------------------------------------------------- request
    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Any = None,
    ) -> Any:
        url = self.config.base_url + path
        headers = self._headers(method, path)  # path only, no query
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            try:
                resp = await client.request(
                    method.upper(),
                    url,
                    params=params,
                    json=json_body,
                    headers=headers,
                )
            except httpx.RequestError as e:
                raise NaverSearchAdAPIError(f"HTTP transport error: {e}") from e

        if resp.status_code >= 400:
            try:
                body = resp.json()
                msg = body.get("title") or body.get("detail") or resp.text
                code = body.get("code")
            except Exception:
                msg = resp.text
                code = None
            logger.warning(
                "Naver API error %s on %s %s: %s", resp.status_code, method, path, msg
            )
            raise NaverSearchAdAPIError(
                f"Naver API {resp.status_code}: {msg}",
                code=code,
                status=resp.status_code,
            )

        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError:
            return resp.text

    # -------------------------------------------------------------- high-level
    async def test_connection(self) -> Dict[str, Any]:
        """Ping a cheap endpoint to validate credentials + signing."""
        data = await self._request("GET", "/ncc/channels")
        channels = data if isinstance(data, list) else []
        return {
            "status": "success",
            "customer_id": self.config.customer_id,
            "business_channel_count": len(channels),
        }

    # Business channels
    async def list_business_channels(self) -> List[Dict[str, Any]]:
        data = await self._request("GET", "/ncc/channels")
        return data or []

    # Campaigns
    async def list_campaigns(self, ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        params = {"ids": ",".join(ids)} if ids else None
        data = await self._request("GET", "/ncc/campaigns", params=params)
        return data or []

    async def get_campaign(self, campaign_id: str) -> Dict[str, Any]:
        return await self._request("GET", f"/ncc/campaigns/{campaign_id}")

    async def create_campaign(
        self,
        name: str,
        campaign_tp: str = "WEB_SITE",
        daily_budget: int = 0,
        use_daily_budget: bool = False,
    ) -> Dict[str, Any]:
        payload = {
            "customerId": int(self.config.customer_id),
            "name": name,
            "campaignTp": campaign_tp,
            "dailyBudget": daily_budget,
            "useDailyBudget": use_daily_budget,
        }
        return await self._request("POST", "/ncc/campaigns", json_body=payload)

    async def set_campaign_status(self, campaign_id: str, paused: bool) -> Dict[str, Any]:
        # Field-scoped PUT — only userLock is applied
        path = f"/ncc/campaigns/{campaign_id}"
        body = {
            "nccCampaignId": campaign_id,
            "customerId": int(self.config.customer_id),
            "userLock": 1 if paused else 0,
        }
        return await self._request("PUT", path, params={"fields": "userLock"}, json_body=body)

    async def delete_campaign(self, campaign_id: str) -> None:
        await self._request("DELETE", f"/ncc/campaigns/{campaign_id}")

    # Ad groups
    async def list_ad_groups(
        self,
        campaign_id: Optional[str] = None,
        ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if campaign_id:
            params["nccCampaignId"] = campaign_id
        if ids:
            params["ids"] = ",".join(ids)
        data = await self._request("GET", "/ncc/adgroups", params=params or None)
        return data or []

    async def get_ad_group(self, ad_group_id: str) -> Dict[str, Any]:
        return await self._request("GET", f"/ncc/adgroups/{ad_group_id}")

    # Keywords
    async def list_keywords(self, ad_group_id: str) -> List[Dict[str, Any]]:
        data = await self._request(
            "GET", "/ncc/keywords", params={"nccAdgroupId": ad_group_id}
        )
        return data or []

    async def update_keyword_bid(self, keyword_id: str, bid_amt: int) -> Dict[str, Any]:
        path = f"/ncc/keywords/{keyword_id}"
        body = {"nccKeywordId": keyword_id, "bidAmt": bid_amt}
        return await self._request("PUT", path, params={"fields": "bidAmt"}, json_body=body)

    # Ads
    async def list_ads(self, ad_group_id: str) -> List[Dict[str, Any]]:
        data = await self._request(
            "GET", "/ncc/ads", params={"nccAdgroupId": ad_group_id}
        )
        return data or []

    # Stats
    async def get_stats(
        self,
        ids: List[str],
        fields: Optional[List[str]] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        date_preset: Optional[str] = None,
    ) -> Any:
        fields = fields or [
            "clkCnt", "impCnt", "salesAmt", "ctr", "cpc", "avgRnk", "ccnt",
        ]
        params: Dict[str, Any] = {
            "ids": ids,  # httpx repeats list params
            "fields": json.dumps(fields),
        }
        if since and until:
            params["timeRange"] = json.dumps({"since": since, "until": until})
        elif date_preset:
            params["datePreset"] = date_preset
        else:
            raise ValueError("provide (since, until) or date_preset")
        return await self._request("GET", "/stats", params=params)

    # Stat reports (async jobs)
    async def create_stat_report(self, report_tp: str, stat_dt: str) -> Dict[str, Any]:
        return await self._request(
            "POST",
            "/stat-reports",
            json_body={"reportTp": report_tp, "statDt": stat_dt},
        )

    async def get_stat_report(self, report_job_id: str) -> Dict[str, Any]:
        return await self._request("GET", f"/stat-reports/{report_job_id}")

    # Keyword tool — search volume / suggestions
    async def keyword_tool(
        self,
        hint_keywords: Optional[List[str]] = None,
        show_detail: bool = True,
    ) -> Any:
        params: Dict[str, Any] = {"showDetail": "1" if show_detail else "0"}
        if hint_keywords:
            params["hintKeywords"] = ",".join(hint_keywords)
        return await self._request("GET", "/keywordstool", params=params)

    # Billing
    async def get_bizmoney_balance(self) -> Dict[str, Any]:
        return await self._request("GET", "/billing/bizmoney")

    async def get_bizmoney_cost(self, date: str) -> Dict[str, Any]:
        return await self._request("GET", f"/billing/bizmoney/cost/{date}")
