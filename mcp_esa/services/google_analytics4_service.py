"""
Google Analytics 4 (GA4) Service Module
Wraps the GA4 Data API + Admin API using OAuth user credentials.
"""

import re
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

try:
    from google.oauth2.credentials import Credentials
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        RunReportRequest,
        RunRealtimeReportRequest,
        Dimension,
        Metric,
        DateRange,
        OrderBy,
        Filter,
        FilterExpression,
    )
    from google.analytics.admin_v1beta import AnalyticsAdminServiceClient
    GA4_AVAILABLE = True
except ImportError:
    GA4_AVAILABLE = False
    Credentials = None
    BetaAnalyticsDataClient = None
    AnalyticsAdminServiceClient = None

logger = logging.getLogger(__name__)

GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
TOKEN_URI = "https://oauth2.googleapis.com/token"
MAX_LIMIT = 10_000
_GA4_NAME_RE = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]{0,99}$')
_DATE_RE = re.compile(r'^(\d{4}-\d{2}-\d{2}|today|yesterday|\d+daysAgo)$')


@dataclass
class GA4Config:
    """GA4 OAuth user-credential config."""
    client_id: str
    client_secret: str
    refresh_token: str


class GA4APIError(Exception):
    """GA4 API error."""
    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.details = details or {}


class GA4Service:
    """Service for Google Analytics 4 Data + Admin APIs."""

    def __init__(self, config: GA4Config):
        if not GA4_AVAILABLE:
            raise ImportError(
                "GA4 libraries not installed. Run: "
                "pip install google-analytics-data google-analytics-admin"
            )
        self.config = config
        self._creds: Optional[Credentials] = None
        self._data_client: Optional[BetaAnalyticsDataClient] = None
        self._admin_client: Optional[AnalyticsAdminServiceClient] = None

    # ------------------------------------------------------------------ creds
    def _get_credentials(self) -> Credentials:
        if self._creds is None:
            if not self.config.refresh_token:
                raise GA4APIError("GA4 refresh token not configured")
            self._creds = Credentials(
                token=None,
                refresh_token=self.config.refresh_token,
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
                token_uri=TOKEN_URI,
                scopes=GA4_SCOPES,
            )
        return self._creds

    def _data(self) -> BetaAnalyticsDataClient:
        if self._data_client is None:
            self._data_client = BetaAnalyticsDataClient(credentials=self._get_credentials())
        return self._data_client

    def _admin(self) -> AnalyticsAdminServiceClient:
        if self._admin_client is None:
            self._admin_client = AnalyticsAdminServiceClient(credentials=self._get_credentials())
        return self._admin_client

    @staticmethod
    def _normalize_property_id(property_id: str) -> str:
        if not property_id:
            raise GA4APIError("property_id is required")
        pid = str(property_id).strip()
        if not pid.startswith("properties/"):
            pid = f"properties/{pid}"
        return pid

    @staticmethod
    def _validate_names(names: List[str], field: str) -> None:
        for n in names:
            if not _GA4_NAME_RE.match(n):
                raise GA4APIError(f"Invalid {field} name: contains disallowed characters")

    @staticmethod
    def _validate_date(value: str, field: str) -> None:
        if not _DATE_RE.match(value):
            raise GA4APIError(f"Invalid {field}: use YYYY-MM-DD, 'today', 'yesterday', or 'NdaysAgo'")

    @staticmethod
    def _clamp_limit(limit: int) -> int:
        return min(max(1, int(limit)), MAX_LIMIT)

    @staticmethod
    def _format_response(resp) -> Dict[str, Any]:
        dim_headers = [h.name for h in resp.dimension_headers]
        met_headers = [h.name for h in resp.metric_headers]
        rows = []
        for row in resp.rows:
            entry = {}
            for i, dim in enumerate(row.dimension_values):
                entry[dim_headers[i]] = dim.value
            for i, met in enumerate(row.metric_values):
                entry[met_headers[i]] = met.value
            rows.append(entry)
        out: Dict[str, Any] = {
            "row_count": getattr(resp, "row_count", len(rows)),
            "dimensions": dim_headers,
            "metrics": met_headers,
            "rows": rows,
        }
        meta = getattr(resp, "metadata", None)
        if meta is not None:
            sampling = list(getattr(meta, "sampling_metadatas", []) or [])
            if sampling:
                out["sampled"] = True
        return out

    # --------------------------------------------------------------- methods
    async def test_connection(self) -> Dict[str, Any]:
        try:
            properties = await self.list_properties()
            first = properties.get("properties", [])
            return {
                "status": "success",
                "property_count": len(first),
                "first_property": first[0]["property_id"] if first else None,
            }
        except Exception as e:
            logger.error("GA4 test_connection failed: %s", e, exc_info=True)
            return {"status": "error", "message": "Connection test failed. Check server logs."}

    async def list_properties(self) -> Dict[str, Any]:
        client = self._admin()
        results = []
        try:
            for summary in client.list_account_summaries():
                account_name = summary.display_name
                account_id = summary.account
                for prop in summary.property_summaries:
                    results.append({
                        "property_id": prop.property,
                        "display_name": prop.display_name,
                        "parent_account": account_id,
                        "parent_account_name": account_name,
                    })
        except Exception as e:
            logger.error("list_account_summaries failed: %s", e, exc_info=True)
            raise GA4APIError("Failed to list GA4 properties")
        return {"status": "success", "properties": results, "count": len(results)}

    async def get_metadata(self, property_id: str) -> Dict[str, Any]:
        pid = self._normalize_property_id(property_id)
        try:
            meta = self._data().get_metadata(name=f"{pid}/metadata")
        except Exception as e:
            logger.error("get_metadata failed: %s", e, exc_info=True)
            raise GA4APIError("Failed to retrieve property metadata")
        dims = [{"api_name": d.api_name, "ui_name": d.ui_name, "category": d.category} for d in meta.dimensions]
        mets = [{"api_name": m.api_name, "ui_name": m.ui_name, "category": m.category, "type": m.type_.name if hasattr(m.type_, "name") else str(m.type_)} for m in meta.metrics]
        return {
            "status": "success",
            "property_id": pid,
            "dimensions": dims,
            "metrics": mets,
            "dimension_count": len(dims),
            "metric_count": len(mets),
        }

    async def run_report(
        self,
        property_id: str,
        dimensions: List[str],
        metrics: List[str],
        start_date: str = "7daysAgo",
        end_date: str = "today",
        limit: int = 100,
        order_by_metric: Optional[str] = None,
        order_desc: bool = True,
    ) -> Dict[str, Any]:
        pid = self._normalize_property_id(property_id)
        if not metrics:
            raise GA4APIError("At least one metric is required")
        self._validate_names(metrics, "metric")
        if dimensions:
            self._validate_names(dimensions, "dimension")
        self._validate_date(start_date, "start_date")
        self._validate_date(end_date, "end_date")
        limit = self._clamp_limit(limit)
        if order_by_metric:
            if order_by_metric not in metrics:
                raise GA4APIError("order_by_metric must be one of the requested metrics")
        req = RunReportRequest(
            property=pid,
            dimensions=[Dimension(name=d) for d in (dimensions or [])],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=limit,
        )
        if order_by_metric:
            req.order_bys = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name=order_by_metric), desc=order_desc)]
        try:
            resp = self._data().run_report(req)
        except Exception as e:
            logger.error("run_report failed: %s", e, exc_info=True)
            raise GA4APIError("GA4 report request failed")
        out = self._format_response(resp)
        out["status"] = "success"
        out["property_id"] = pid
        out["date_range"] = {"start_date": start_date, "end_date": end_date}
        return out

    async def run_realtime(
        self,
        property_id: str,
        dimensions: Optional[List[str]] = None,
        metrics: Optional[List[str]] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        pid = self._normalize_property_id(property_id)
        dims = dimensions or ["country"]
        mets = metrics or ["activeUsers"]
        self._validate_names(dims, "dimension")
        self._validate_names(mets, "metric")
        limit = self._clamp_limit(limit)
        req = RunRealtimeReportRequest(
            property=pid,
            dimensions=[Dimension(name=d) for d in dims],
            metrics=[Metric(name=m) for m in mets],
            limit=limit,
        )
        try:
            resp = self._data().run_realtime_report(req)
        except Exception as e:
            logger.error("run_realtime_report failed: %s", e, exc_info=True)
            raise GA4APIError("GA4 realtime report request failed")
        out = self._format_response(resp)
        out["status"] = "success"
        out["property_id"] = pid
        return out

    # ---------- pre-built convenience reports ----------

    async def top_pages(self, property_id: str, start_date: str = "7daysAgo", end_date: str = "today", limit: int = 25) -> Dict[str, Any]:
        return await self.run_report(
            property_id=property_id,
            dimensions=["pagePath", "pageTitle"],
            metrics=["screenPageViews", "activeUsers", "averageSessionDuration"],
            start_date=start_date, end_date=end_date, limit=limit,
            order_by_metric="screenPageViews",
        )

    async def traffic_sources(self, property_id: str, start_date: str = "7daysAgo", end_date: str = "today", limit: int = 25) -> Dict[str, Any]:
        return await self.run_report(
            property_id=property_id,
            dimensions=["sessionSource", "sessionMedium"],
            metrics=["sessions", "activeUsers", "engagementRate"],
            start_date=start_date, end_date=end_date, limit=limit,
            order_by_metric="sessions",
        )

    async def user_acquisition(self, property_id: str, start_date: str = "7daysAgo", end_date: str = "today", limit: int = 25) -> Dict[str, Any]:
        return await self.run_report(
            property_id=property_id,
            dimensions=["firstUserSource", "firstUserMedium"],
            metrics=["newUsers", "activeUsers"],
            start_date=start_date, end_date=end_date, limit=limit,
            order_by_metric="newUsers",
        )

    async def conversions(self, property_id: str, start_date: str = "7daysAgo", end_date: str = "today", limit: int = 25) -> Dict[str, Any]:
        pid = self._normalize_property_id(property_id)
        self._validate_date(start_date, "start_date")
        self._validate_date(end_date, "end_date")
        limit = self._clamp_limit(limit)
        req = RunReportRequest(
            property=pid,
            dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name="conversions"), Metric(name="eventCount"), Metric(name="totalRevenue")],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=limit,
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="conversions"), desc=True)],
        )
        try:
            resp = self._data().run_report(req)
        except Exception as e:
            logger.error("conversions report failed: %s", e, exc_info=True)
            raise GA4APIError("GA4 conversions report failed")
        out = self._format_response(resp)
        out["status"] = "success"
        out["property_id"] = pid
        out["date_range"] = {"start_date": start_date, "end_date": end_date}
        return out

    async def device_breakdown(self, property_id: str, start_date: str = "7daysAgo", end_date: str = "today", limit: int = 50) -> Dict[str, Any]:
        return await self.run_report(
            property_id=property_id,
            dimensions=["deviceCategory", "operatingSystem"],
            metrics=["activeUsers", "sessions", "engagementRate"],
            start_date=start_date, end_date=end_date, limit=limit,
            order_by_metric="activeUsers",
        )

    async def geo_breakdown(self, property_id: str, start_date: str = "7daysAgo", end_date: str = "today", limit: int = 50) -> Dict[str, Any]:
        return await self.run_report(
            property_id=property_id,
            dimensions=["country", "city"],
            metrics=["activeUsers", "sessions", "engagementRate"],
            start_date=start_date, end_date=end_date, limit=limit,
            order_by_metric="activeUsers",
        )
