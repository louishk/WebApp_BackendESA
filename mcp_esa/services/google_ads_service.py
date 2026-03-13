"""
Google Ads Service Module
Handles Google Ads API integration and operations using the official google-ads library
"""

import asyncio
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
    GOOGLE_ADS_AVAILABLE = True
except ImportError:
    GOOGLE_ADS_AVAILABLE = False
    GoogleAdsClient = None
    GoogleAdsException = Exception

logger = logging.getLogger(__name__)


@dataclass
class GoogleAdsConfig:
    """Google Ads API configuration"""
    client_id: str
    client_secret: str
    refresh_token: str
    developer_token: str
    login_customer_id: Optional[str] = None
    timeout: int = 60

    def __post_init__(self):
        # Remove dashes from login_customer_id if present
        if self.login_customer_id:
            self.login_customer_id = self.login_customer_id.replace('-', '')


class GoogleAdsAPIError(Exception):
    """Custom exception for Google Ads API errors"""
    def __init__(self, message: str, error_code: str = None, details: dict = None):
        super().__init__(message)
        self.error_code = error_code
        self.details = details or {}


class GoogleAdsService:
    """Service for interacting with Google Ads API"""

    def __init__(self, config: GoogleAdsConfig):
        if not GOOGLE_ADS_AVAILABLE:
            raise ImportError(
                "google-ads library is required for Google Ads integration. "
                "Install with: pip install google-ads>=25.0.0"
            )

        self.config = config
        self._client: Optional[GoogleAdsClient] = None

    def _get_client(self) -> GoogleAdsClient:
        """Get or create the Google Ads client"""
        if self._client is None:
            credentials_dict = {
                "developer_token": self.config.developer_token,
                "refresh_token": self.config.refresh_token,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "use_proto_plus": True
            }

            # Add login_customer_id if provided (required for manager accounts)
            if self.config.login_customer_id:
                credentials_dict["login_customer_id"] = self.config.login_customer_id

            self._client = GoogleAdsClient.load_from_dict(credentials_dict)

        return self._client

    def _format_customer_id(self, customer_id: str) -> str:
        """Ensure customer ID is in correct format (no dashes)"""
        cid = customer_id.replace('-', '')
        if not cid.isdigit():
            raise ValueError("customer_id must be numeric")
        return cid

    @staticmethod
    def _validate_numeric_id(value: str, label: str = "ID") -> str:
        """Validate that a value is numeric (for safe GAQL interpolation)."""
        if not str(value).isdigit():
            raise ValueError(f"{label} must be numeric")
        return str(value)

    _VALID_DATE_RANGES = {
        'TODAY', 'YESTERDAY', 'LAST_7_DAYS', 'LAST_BUSINESS_WEEK',
        'THIS_MONTH', 'LAST_MONTH', 'LAST_14_DAYS', 'LAST_30_DAYS',
        'LAST_WEEK_SUN_SAT', 'LAST_WEEK_MON_SUN', 'THIS_WEEK_SUN_TODAY',
        'THIS_WEEK_MON_TODAY', 'LAST_QUARTER', 'THIS_QUARTER', 'LAST_YEAR',
        'THIS_YEAR',
    }

    @classmethod
    def _validate_date_range(cls, value: str) -> str:
        """Validate GAQL date range constant."""
        upper = value.upper().strip()
        if upper not in cls._VALID_DATE_RANGES:
            raise ValueError(f"Invalid date_range: must be one of {sorted(cls._VALID_DATE_RANGES)}")
        return upper

    def _parse_google_ads_exception(self, ex: GoogleAdsException) -> GoogleAdsAPIError:
        """Parse GoogleAdsException into a more readable error"""
        error_messages = []
        error_code = None

        for error in ex.failure.errors:
            error_messages.append(error.message)
            if error.error_code:
                # Get the error code name
                error_code = str(error.error_code)

        message = "; ".join(error_messages) if error_messages else str(ex)
        return GoogleAdsAPIError(
            message=message,
            error_code=error_code,
            details={"request_id": ex.request_id if hasattr(ex, 'request_id') else None}
        )

    async def test_connection(self) -> Dict[str, Any]:
        """Test Google Ads API connectivity"""
        try:
            client = self._get_client()
            customer_service = client.get_service("CustomerService")

            # List accessible customers
            accessible_customers = await asyncio.to_thread(
                customer_service.list_accessible_customers
            )

            customer_count = len(accessible_customers.resource_names)

            return {
                "status": "success",
                "message": "Google Ads API connection successful",
                "accessible_customers": customer_count,
                "developer_token_status": "valid",
                "login_customer_id": self.config.login_customer_id
            }

        except GoogleAdsException as ex:
            error = self._parse_google_ads_exception(ex)
            return {
                "status": "error",
                "message": f"Google Ads API error: {error.message}",
                "error_code": error.error_code
            }
        except Exception as e:
            logger.error(f"Google Ads connection test failed: {e}")
            return {
                "status": "error",
                "message": "Connection test failed"
            }

    async def list_accessible_customers(self) -> Dict[str, Any]:
        """List all accessible Google Ads accounts"""
        try:
            client = self._get_client()
            customer_service = client.get_service("CustomerService")

            accessible_customers = await asyncio.to_thread(
                customer_service.list_accessible_customers
            )

            # Parse resource names to get customer IDs
            customer_ids = []
            for resource_name in accessible_customers.resource_names:
                # Resource name format: "customers/1234567890"
                customer_id = resource_name.split("/")[-1]
                customer_ids.append(customer_id)

            return {
                "status": "success",
                "customer_ids": customer_ids,
                "count": len(customer_ids)
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_account_info(self, customer_id: str) -> Dict[str, Any]:
        """Get details for a specific Google Ads account"""
        customer_id = self._format_customer_id(customer_id)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = """
                SELECT
                    customer.id,
                    customer.descriptive_name,
                    customer.currency_code,
                    customer.time_zone,
                    customer.manager,
                    customer.status
                FROM customer
                LIMIT 1
            """

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            for row in response:
                customer = row.customer
                return {
                    "status": "success",
                    "account": {
                        "id": str(customer.id),
                        "name": customer.descriptive_name,
                        "currency_code": customer.currency_code,
                        "time_zone": customer.time_zone,
                        "is_manager": customer.manager,
                        "status": customer.status.name
                    }
                }

            return {"status": "error", "message": "No account data returned"}

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def execute_query(self, customer_id: str, query: str) -> Dict[str, Any]:
        """Execute a GAQL (Google Ads Query Language) query"""
        customer_id = self._format_customer_id(customer_id)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            # Convert response to list of dicts
            results = []
            for row in response:
                row_dict = self._proto_to_dict(row)
                results.append(row_dict)

            return {
                "status": "success",
                "row_count": len(results),
                "results": results
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    def _proto_to_dict(self, proto_obj) -> Dict[str, Any]:
        """Convert a protobuf object to a dictionary"""
        result = {}

        # Get all fields from the proto object
        for field in proto_obj._pb.DESCRIPTOR.fields:
            field_name = field.name
            try:
                value = getattr(proto_obj, field_name)

                # Handle nested proto objects
                if hasattr(value, '_pb'):
                    result[field_name] = self._proto_to_dict(value)
                # Handle repeated fields
                elif hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):
                    result[field_name] = [
                        self._proto_to_dict(item) if hasattr(item, '_pb') else item
                        for item in value
                    ]
                # Handle enum values
                elif hasattr(value, 'name'):
                    result[field_name] = value.name
                else:
                    result[field_name] = value
            except Exception:
                continue

        return result

    async def list_campaigns(
        self,
        customer_id: str,
        include_metrics: bool = False,
        status_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """List campaigns for a customer account"""
        customer_id = self._format_customer_id(customer_id)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            # Build query
            select_fields = [
                "campaign.id",
                "campaign.name",
                "campaign.status",
                "campaign.advertising_channel_type"
            ]

            if include_metrics:
                select_fields.extend([
                    "metrics.impressions",
                    "metrics.clicks",
                    "metrics.cost_micros",
                    "metrics.conversions"
                ])

            query = f"SELECT {', '.join(select_fields)} FROM campaign"

            if status_filter:
                _VALID_STATUSES = {'ENABLED', 'PAUSED', 'REMOVED'}
                status_val = status_filter.upper()
                if status_val not in _VALID_STATUSES:
                    raise ValueError(f"Invalid status_filter: must be one of {_VALID_STATUSES}")
                query += f" WHERE campaign.status = '{status_val}'"

            query += " ORDER BY campaign.name"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            campaigns = []
            for row in response:
                campaign_data = {
                    "id": str(row.campaign.id),
                    "name": row.campaign.name,
                    "status": row.campaign.status.name,
                    "channel_type": row.campaign.advertising_channel_type.name
                }

                if include_metrics:
                    campaign_data["metrics"] = {
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                        "cost": row.metrics.cost_micros / 1_000_000,  # Convert micros to currency
                        "conversions": row.metrics.conversions
                    }

                campaigns.append(campaign_data)

            return {
                "status": "success",
                "campaign_count": len(campaigns),
                "campaigns": campaigns
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_campaign(self, customer_id: str, campaign_id: str) -> Dict[str, Any]:
        """Get details for a specific campaign"""
        customer_id = self._format_customer_id(customer_id)
        campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    campaign.id,
                    campaign.name,
                    campaign.status,
                    campaign.advertising_channel_type,
                    campaign.advertising_channel_sub_type,
                    campaign.bidding_strategy_type,
                    campaign.campaign_budget,
                    campaign_budget.amount_micros,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.ctr,
                    metrics.average_cpc
                FROM campaign
                WHERE campaign.id = {campaign_id}
            """

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            for row in response:
                return {
                    "status": "success",
                    "campaign": {
                        "id": str(row.campaign.id),
                        "name": row.campaign.name,
                        "status": row.campaign.status.name,
                        "channel_type": row.campaign.advertising_channel_type.name,
                        "channel_sub_type": row.campaign.advertising_channel_sub_type.name,
                        "bidding_strategy_type": row.campaign.bidding_strategy_type.name,
                        "budget_micros": row.campaign_budget.amount_micros,
                        "budget": row.campaign_budget.amount_micros / 1_000_000,
                        "metrics": {
                            "impressions": row.metrics.impressions,
                            "clicks": row.metrics.clicks,
                            "cost": row.metrics.cost_micros / 1_000_000,
                            "conversions": row.metrics.conversions,
                            "ctr": row.metrics.ctr,
                            "average_cpc": row.metrics.average_cpc / 1_000_000
                        }
                    }
                }

            return {"status": "error", "message": f"Campaign {campaign_id} not found"}

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def create_campaign(
        self,
        customer_id: str,
        name: str,
        budget_amount: float,
        advertising_channel_type: str = "SEARCH",
        status: str = "PAUSED"
    ) -> Dict[str, Any]:
        """Create a new campaign"""
        customer_id = self._format_customer_id(customer_id)

        try:
            client = self._get_client()

            # Create budget first
            campaign_budget_service = client.get_service("CampaignBudgetService")
            campaign_budget_operation = client.get_type("CampaignBudgetOperation")
            campaign_budget = campaign_budget_operation.create

            campaign_budget.name = f"Budget for {name}"
            campaign_budget.amount_micros = int(budget_amount * 1_000_000)
            campaign_budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD

            # Create the budget
            budget_response = await asyncio.to_thread(
                campaign_budget_service.mutate_campaign_budgets,
                customer_id=customer_id,
                operations=[campaign_budget_operation]
            )
            budget_resource_name = budget_response.results[0].resource_name

            # Create campaign
            campaign_service = client.get_service("CampaignService")
            campaign_operation = client.get_type("CampaignOperation")
            campaign = campaign_operation.create

            campaign.name = name
            campaign.campaign_budget = budget_resource_name
            campaign.advertising_channel_type = getattr(
                client.enums.AdvertisingChannelTypeEnum, advertising_channel_type
            )
            campaign.status = getattr(client.enums.CampaignStatusEnum, status)

            # Set bidding strategy (manual CPC for simplicity)
            campaign.manual_cpc.enhanced_cpc_enabled = False

            # Set network settings for Search campaigns
            if advertising_channel_type == "SEARCH":
                campaign.network_settings.target_google_search = True
                campaign.network_settings.target_search_network = True
                campaign.network_settings.target_content_network = False

            campaign_response = await asyncio.to_thread(
                campaign_service.mutate_campaigns,
                customer_id=customer_id,
                operations=[campaign_operation]
            )

            campaign_resource_name = campaign_response.results[0].resource_name
            campaign_id = campaign_resource_name.split("/")[-1]

            return {
                "status": "success",
                "message": f"Campaign '{name}' created successfully",
                "campaign_id": campaign_id,
                "resource_name": campaign_resource_name,
                "budget_resource_name": budget_resource_name
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def update_campaign(
        self,
        customer_id: str,
        campaign_id: str,
        name: Optional[str] = None,
        status: Optional[str] = None,
        budget_amount: Optional[float] = None
    ) -> Dict[str, Any]:
        """Update campaign settings"""
        customer_id = self._format_customer_id(customer_id)
        campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")

        try:
            client = self._get_client()
            campaign_service = client.get_service("CampaignService")

            # Build the campaign operation
            campaign_operation = client.get_type("CampaignOperation")
            campaign = campaign_operation.update

            campaign.resource_name = client.get_service(
                "CampaignService"
            ).campaign_path(customer_id, campaign_id)

            # Track which fields to update
            field_mask_paths = []

            if name:
                campaign.name = name
                field_mask_paths.append("name")

            if status:
                campaign.status = getattr(client.enums.CampaignStatusEnum, status.upper())
                field_mask_paths.append("status")

            if not field_mask_paths:
                return {"status": "error", "message": "No fields to update specified"}

            # Set field mask
            client.copy_from(
                campaign_operation.update_mask,
                client.get_type("FieldMask")(paths=field_mask_paths)
            )

            response = await asyncio.to_thread(
                campaign_service.mutate_campaigns,
                customer_id=customer_id,
                operations=[campaign_operation]
            )

            # Update budget if specified
            if budget_amount is not None:
                await self._update_campaign_budget(
                    client, customer_id, campaign_id, budget_amount
                )

            return {
                "status": "success",
                "message": f"Campaign {campaign_id} updated successfully",
                "resource_name": response.results[0].resource_name
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def _update_campaign_budget(
        self,
        client: GoogleAdsClient,
        customer_id: str,
        campaign_id: str,
        budget_amount: float
    ) -> None:
        """Helper to update a campaign's budget"""
        campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
        ga_service = client.get_service("GoogleAdsService")

        # Get the budget resource name
        query = f"""
            SELECT campaign.campaign_budget
            FROM campaign
            WHERE campaign.id = {campaign_id}
        """

        response = await asyncio.to_thread(
            ga_service.search,
            customer_id=customer_id,
            query=query
        )

        budget_resource_name = None
        for row in response:
            budget_resource_name = row.campaign.campaign_budget

        if not budget_resource_name:
            raise GoogleAdsAPIError("Could not find campaign budget")

        # Update the budget
        budget_service = client.get_service("CampaignBudgetService")
        budget_operation = client.get_type("CampaignBudgetOperation")
        budget = budget_operation.update

        budget.resource_name = budget_resource_name
        budget.amount_micros = int(budget_amount * 1_000_000)

        client.copy_from(
            budget_operation.update_mask,
            client.get_type("FieldMask")(paths=["amount_micros"])
        )

        await asyncio.to_thread(
            budget_service.mutate_campaign_budgets,
            customer_id=customer_id,
            operations=[budget_operation]
        )

    async def set_campaign_status(
        self,
        customer_id: str,
        campaign_id: str,
        status: str
    ) -> Dict[str, Any]:
        """Change campaign status (ENABLED, PAUSED, REMOVED)"""
        return await self.update_campaign(
            customer_id=customer_id,
            campaign_id=campaign_id,
            status=status
        )

    async def list_ad_groups(
        self,
        customer_id: str,
        campaign_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """List ad groups, optionally filtered by campaign"""
        customer_id = self._format_customer_id(customer_id)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = """
                SELECT
                    ad_group.id,
                    ad_group.name,
                    ad_group.status,
                    ad_group.type,
                    campaign.id,
                    campaign.name,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros
                FROM ad_group
            """

            if campaign_id:
                campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
                query += f" WHERE campaign.id = {campaign_id}"

            query += " ORDER BY ad_group.name"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            ad_groups = []
            for row in response:
                ad_groups.append({
                    "id": str(row.ad_group.id),
                    "name": row.ad_group.name,
                    "status": row.ad_group.status.name,
                    "type": row.ad_group.type_.name,
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "metrics": {
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                        "cost": row.metrics.cost_micros / 1_000_000
                    }
                })

            return {
                "status": "success",
                "ad_group_count": len(ad_groups),
                "ad_groups": ad_groups
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def create_ad_group(
        self,
        customer_id: str,
        campaign_id: str,
        name: str,
        cpc_bid_micros: int = 1_000_000,
        status: str = "ENABLED"
    ) -> Dict[str, Any]:
        """Create a new ad group"""
        customer_id = self._format_customer_id(customer_id)
        campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")

        try:
            client = self._get_client()
            ad_group_service = client.get_service("AdGroupService")

            ad_group_operation = client.get_type("AdGroupOperation")
            ad_group = ad_group_operation.create

            ad_group.name = name
            ad_group.status = getattr(client.enums.AdGroupStatusEnum, status)
            ad_group.campaign = client.get_service("CampaignService").campaign_path(
                customer_id, campaign_id
            )
            ad_group.type_ = client.enums.AdGroupTypeEnum.SEARCH_STANDARD
            ad_group.cpc_bid_micros = cpc_bid_micros

            response = await asyncio.to_thread(
                ad_group_service.mutate_ad_groups,
                customer_id=customer_id,
                operations=[ad_group_operation]
            )

            resource_name = response.results[0].resource_name
            ad_group_id = resource_name.split("/")[-1]

            return {
                "status": "success",
                "message": f"Ad group '{name}' created successfully",
                "ad_group_id": ad_group_id,
                "resource_name": resource_name
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def update_ad_group(
        self,
        customer_id: str,
        ad_group_id: str,
        name: Optional[str] = None,
        status: Optional[str] = None,
        cpc_bid_micros: Optional[int] = None
    ) -> Dict[str, Any]:
        """Update ad group settings"""
        customer_id = self._format_customer_id(customer_id)
        ad_group_id = self._validate_numeric_id(ad_group_id, "ad_group_id")

        try:
            client = self._get_client()
            ad_group_service = client.get_service("AdGroupService")

            ad_group_operation = client.get_type("AdGroupOperation")
            ad_group = ad_group_operation.update

            ad_group.resource_name = client.get_service(
                "AdGroupService"
            ).ad_group_path(customer_id, ad_group_id)

            field_mask_paths = []

            if name:
                ad_group.name = name
                field_mask_paths.append("name")

            if status:
                ad_group.status = getattr(client.enums.AdGroupStatusEnum, status.upper())
                field_mask_paths.append("status")

            if cpc_bid_micros is not None:
                ad_group.cpc_bid_micros = cpc_bid_micros
                field_mask_paths.append("cpc_bid_micros")

            if not field_mask_paths:
                return {"status": "error", "message": "No fields to update specified"}

            client.copy_from(
                ad_group_operation.update_mask,
                client.get_type("FieldMask")(paths=field_mask_paths)
            )

            response = await asyncio.to_thread(
                ad_group_service.mutate_ad_groups,
                customer_id=customer_id,
                operations=[ad_group_operation]
            )

            return {
                "status": "success",
                "message": f"Ad group {ad_group_id} updated successfully",
                "resource_name": response.results[0].resource_name
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_campaign_performance(
        self,
        customer_id: str,
        date_range: str = "LAST_30_DAYS",
        campaign_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get campaign performance metrics"""
        customer_id = self._format_customer_id(customer_id)
        date_range = self._validate_date_range(date_range)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    campaign.id,
                    campaign.name,
                    campaign.status,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.ctr,
                    metrics.cost_micros,
                    metrics.average_cpc,
                    metrics.conversions,
                    metrics.conversions_value,
                    metrics.cost_per_conversion
                FROM campaign
                WHERE segments.date DURING {date_range}
            """

            if campaign_id:
                campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
                query += f" AND campaign.id = {campaign_id}"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            campaigns = []
            for row in response:
                campaigns.append({
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "status": row.campaign.status.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "ctr": round(row.metrics.ctr * 100, 2),
                    "cost": round(row.metrics.cost_micros / 1_000_000, 2),
                    "average_cpc": round(row.metrics.average_cpc / 1_000_000, 2),
                    "conversions": row.metrics.conversions,
                    "conversion_value": round(row.metrics.conversions_value, 2),
                    "cost_per_conversion": round(row.metrics.cost_per_conversion / 1_000_000, 2) if row.metrics.cost_per_conversion else 0
                })

            return {
                "status": "success",
                "date_range": date_range,
                "campaign_count": len(campaigns),
                "campaigns": campaigns
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_account_performance(
        self,
        customer_id: str,
        date_range: str = "LAST_30_DAYS"
    ) -> Dict[str, Any]:
        """Get account-level performance summary"""
        customer_id = self._format_customer_id(customer_id)
        date_range = self._validate_date_range(date_range)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    customer.id,
                    customer.descriptive_name,
                    customer.currency_code,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.ctr,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.conversions_value
                FROM customer
                WHERE segments.date DURING {date_range}
            """

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            for row in response:
                return {
                    "status": "success",
                    "date_range": date_range,
                    "account": {
                        "id": str(row.customer.id),
                        "name": row.customer.descriptive_name,
                        "currency": row.customer.currency_code
                    },
                    "metrics": {
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                        "ctr": round(row.metrics.ctr * 100, 2),
                        "cost": round(row.metrics.cost_micros / 1_000_000, 2),
                        "conversions": row.metrics.conversions,
                        "conversion_value": round(row.metrics.conversions_value, 2)
                    }
                }

            return {"status": "error", "message": "No account metrics returned"}

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_keyword_performance(
        self,
        customer_id: str,
        campaign_id: Optional[str] = None,
        ad_group_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS"
    ) -> Dict[str, Any]:
        """Get keyword-level performance metrics"""
        customer_id = self._format_customer_id(customer_id)
        date_range = self._validate_date_range(date_range)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    ad_group_criterion.keyword.text,
                    ad_group_criterion.keyword.match_type,
                    ad_group_criterion.status,
                    campaign.id,
                    campaign.name,
                    ad_group.id,
                    ad_group.name,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.ctr,
                    metrics.cost_micros,
                    metrics.average_cpc,
                    metrics.conversions
                FROM keyword_view
                WHERE segments.date DURING {date_range}
            """

            if campaign_id:
                campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
                query += f" AND campaign.id = {campaign_id}"

            if ad_group_id:
                ad_group_id = self._validate_numeric_id(ad_group_id, "ad_group_id")
                query += f" AND ad_group.id = {ad_group_id}"

            query += " ORDER BY metrics.impressions DESC LIMIT 100"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            keywords = []
            for row in response:
                keywords.append({
                    "keyword": row.ad_group_criterion.keyword.text,
                    "match_type": row.ad_group_criterion.keyword.match_type.name,
                    "status": row.ad_group_criterion.status.name,
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "ad_group_id": str(row.ad_group.id),
                    "ad_group_name": row.ad_group.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "ctr": round(row.metrics.ctr * 100, 2),
                    "cost": round(row.metrics.cost_micros / 1_000_000, 2),
                    "average_cpc": round(row.metrics.average_cpc / 1_000_000, 2),
                    "conversions": row.metrics.conversions
                })

            return {
                "status": "success",
                "date_range": date_range,
                "keyword_count": len(keywords),
                "keywords": keywords
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    # =========================================================================
    # AI ANALYSIS DATA METHODS
    # =========================================================================

    async def get_search_terms(
        self,
        customer_id: str,
        campaign_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS",
        limit: int = 500
    ) -> Dict[str, Any]:
        """Fetch search term report - actual user search queries"""
        customer_id = self._format_customer_id(customer_id)
        date_range = self._validate_date_range(date_range)
        limit = max(1, min(int(limit), 10000))

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    search_term_view.search_term,
                    search_term_view.status,
                    campaign.id,
                    campaign.name,
                    ad_group.id,
                    ad_group.name,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.ctr,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.conversions_value
                FROM search_term_view
                WHERE segments.date DURING {date_range}
            """

            if campaign_id:
                campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
                query += f" AND campaign.id = {campaign_id}"

            query += f" ORDER BY metrics.impressions DESC LIMIT {limit}"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            search_terms = []
            for row in response:
                search_terms.append({
                    "search_term": row.search_term_view.search_term,
                    "status": row.search_term_view.status.name,
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "ad_group_id": str(row.ad_group.id),
                    "ad_group_name": row.ad_group.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "ctr": round(row.metrics.ctr * 100, 2) if row.metrics.ctr else 0,
                    "cost": round(row.metrics.cost_micros / 1_000_000, 2),
                    "conversions": row.metrics.conversions,
                    "conversion_value": round(row.metrics.conversions_value, 2)
                })

            return {
                "status": "success",
                "date_range": date_range,
                "search_term_count": len(search_terms),
                "search_terms": search_terms
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_quality_scores(
        self,
        customer_id: str,
        campaign_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch quality score data with all components"""
        customer_id = self._format_customer_id(customer_id)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = """
                SELECT
                    ad_group_criterion.keyword.text,
                    ad_group_criterion.keyword.match_type,
                    ad_group_criterion.quality_info.quality_score,
                    ad_group_criterion.quality_info.creative_quality_score,
                    ad_group_criterion.quality_info.post_click_quality_score,
                    ad_group_criterion.quality_info.search_predicted_ctr,
                    campaign.id,
                    campaign.name,
                    ad_group.id,
                    ad_group.name,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros
                FROM keyword_view
                WHERE ad_group_criterion.status = 'ENABLED'
            """

            if campaign_id:
                campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
                query += f" AND campaign.id = {campaign_id}"

            query += " ORDER BY metrics.impressions DESC LIMIT 200"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            quality_scores = []
            for row in response:
                qi = row.ad_group_criterion.quality_info
                quality_scores.append({
                    "keyword": row.ad_group_criterion.keyword.text,
                    "match_type": row.ad_group_criterion.keyword.match_type.name,
                    "quality_score": qi.quality_score if qi.quality_score else None,
                    "creative_quality": qi.creative_quality_score.name if qi.creative_quality_score else None,
                    "landing_page_quality": qi.post_click_quality_score.name if qi.post_click_quality_score else None,
                    "expected_ctr": qi.search_predicted_ctr.name if qi.search_predicted_ctr else None,
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "ad_group_id": str(row.ad_group.id),
                    "ad_group_name": row.ad_group.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "cost": round(row.metrics.cost_micros / 1_000_000, 2)
                })

            return {
                "status": "success",
                "keyword_count": len(quality_scores),
                "quality_scores": quality_scores
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_auction_insights(
        self,
        customer_id: str,
        campaign_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS"
    ) -> Dict[str, Any]:
        """Fetch auction insights - competitor data"""
        customer_id = self._format_customer_id(customer_id)
        date_range = self._validate_date_range(date_range)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            # Auction insights query - aggregated by domain
            query = f"""
                SELECT
                    segments.auction_insight_domain,
                    metrics.auction_insight_search_impression_share,
                    metrics.auction_insight_search_overlap_rate,
                    metrics.auction_insight_search_position_above_rate,
                    metrics.auction_insight_search_top_impression_percentage,
                    metrics.auction_insight_search_absolute_top_impression_percentage,
                    metrics.auction_insight_search_outranking_share
                FROM campaign
                WHERE segments.date DURING {date_range}
            """

            if campaign_id:
                campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
                query += f" AND campaign.id = {campaign_id}"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            insights = []
            for row in response:
                insights.append({
                    "domain": row.segments.auction_insight_domain,
                    "impression_share": round(row.metrics.auction_insight_search_impression_share * 100, 2) if row.metrics.auction_insight_search_impression_share else None,
                    "overlap_rate": round(row.metrics.auction_insight_search_overlap_rate * 100, 2) if row.metrics.auction_insight_search_overlap_rate else None,
                    "position_above_rate": round(row.metrics.auction_insight_search_position_above_rate * 100, 2) if row.metrics.auction_insight_search_position_above_rate else None,
                    "top_impression_pct": round(row.metrics.auction_insight_search_top_impression_percentage * 100, 2) if row.metrics.auction_insight_search_top_impression_percentage else None,
                    "abs_top_impression_pct": round(row.metrics.auction_insight_search_absolute_top_impression_percentage * 100, 2) if row.metrics.auction_insight_search_absolute_top_impression_percentage else None,
                    "outranking_share": round(row.metrics.auction_insight_search_outranking_share * 100, 2) if row.metrics.auction_insight_search_outranking_share else None
                })

            return {
                "status": "success",
                "date_range": date_range,
                "competitor_count": len(insights),
                "auction_insights": insights
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_negative_keywords(
        self,
        customer_id: str,
        campaign_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch negative keywords from campaigns"""
        customer_id = self._format_customer_id(customer_id)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = """
                SELECT
                    campaign_criterion.keyword.text,
                    campaign_criterion.keyword.match_type,
                    campaign_criterion.negative,
                    campaign.id,
                    campaign.name
                FROM campaign_criterion
                WHERE campaign_criterion.negative = TRUE
                  AND campaign_criterion.type = 'KEYWORD'
            """

            if campaign_id:
                campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
                query += f" AND campaign.id = {campaign_id}"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            negative_keywords = []
            for row in response:
                negative_keywords.append({
                    "keyword": row.campaign_criterion.keyword.text,
                    "match_type": row.campaign_criterion.keyword.match_type.name,
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name
                })

            return {
                "status": "success",
                "negative_keyword_count": len(negative_keywords),
                "negative_keywords": negative_keywords
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_geographic_performance(
        self,
        customer_id: str,
        campaign_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS",
        limit: int = 100
    ) -> Dict[str, Any]:
        """Fetch geographic performance data"""
        customer_id = self._format_customer_id(customer_id)
        date_range = self._validate_date_range(date_range)
        limit = max(1, min(int(limit), 10000))

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    geographic_view.country_criterion_id,
                    geographic_view.location_type,
                    campaign.id,
                    campaign.name,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.ctr,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.conversions_value
                FROM geographic_view
                WHERE segments.date DURING {date_range}
            """

            if campaign_id:
                campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
                query += f" AND campaign.id = {campaign_id}"

            query += f" ORDER BY metrics.cost_micros DESC LIMIT {limit}"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            geo_data = []
            for row in response:
                geo_data.append({
                    "country_criterion_id": row.geographic_view.country_criterion_id,
                    "location_type": row.geographic_view.location_type.name,
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "ctr": round(row.metrics.ctr * 100, 2) if row.metrics.ctr else 0,
                    "cost": round(row.metrics.cost_micros / 1_000_000, 2),
                    "conversions": row.metrics.conversions,
                    "conversion_value": round(row.metrics.conversions_value, 2)
                })

            return {
                "status": "success",
                "date_range": date_range,
                "location_count": len(geo_data),
                "geographic_data": geo_data
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_device_performance(
        self,
        customer_id: str,
        campaign_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS"
    ) -> Dict[str, Any]:
        """Fetch device performance breakdown"""
        customer_id = self._format_customer_id(customer_id)
        date_range = self._validate_date_range(date_range)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    segments.device,
                    campaign.id,
                    campaign.name,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.ctr,
                    metrics.cost_micros,
                    metrics.average_cpc,
                    metrics.conversions,
                    metrics.conversions_value
                FROM campaign
                WHERE segments.date DURING {date_range}
            """

            if campaign_id:
                campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
                query += f" AND campaign.id = {campaign_id}"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            device_data = []
            for row in response:
                device_data.append({
                    "device": row.segments.device.name,
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "ctr": round(row.metrics.ctr * 100, 2) if row.metrics.ctr else 0,
                    "cost": round(row.metrics.cost_micros / 1_000_000, 2),
                    "average_cpc": round(row.metrics.average_cpc / 1_000_000, 2) if row.metrics.average_cpc else 0,
                    "conversions": row.metrics.conversions,
                    "conversion_value": round(row.metrics.conversions_value, 2)
                })

            return {
                "status": "success",
                "date_range": date_range,
                "device_breakdown": device_data
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_ad_performance(
        self,
        customer_id: str,
        campaign_id: Optional[str] = None,
        ad_group_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS",
        limit: int = 100
    ) -> Dict[str, Any]:
        """Fetch individual ad performance metrics"""
        customer_id = self._format_customer_id(customer_id)
        date_range = self._validate_date_range(date_range)
        limit = max(1, min(int(limit), 10000))

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    ad_group_ad.ad.id,
                    ad_group_ad.ad.type,
                    ad_group_ad.ad.final_urls,
                    ad_group_ad.ad.responsive_search_ad.headlines,
                    ad_group_ad.ad.responsive_search_ad.descriptions,
                    ad_group_ad.status,
                    ad_group_ad.ad_strength,
                    campaign.id,
                    campaign.name,
                    ad_group.id,
                    ad_group.name,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.ctr,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.conversions_value
                FROM ad_group_ad
                WHERE segments.date DURING {date_range}
                  AND ad_group_ad.status != 'REMOVED'
            """

            if campaign_id:
                campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
                query += f" AND campaign.id = {campaign_id}"

            if ad_group_id:
                ad_group_id = self._validate_numeric_id(ad_group_id, "ad_group_id")
                query += f" AND ad_group.id = {ad_group_id}"

            query += f" ORDER BY metrics.impressions DESC LIMIT {limit}"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            ads = []
            for row in response:
                ad = row.ad_group_ad.ad
                # Extract headlines and descriptions for RSA
                headlines = []
                descriptions = []
                if ad.responsive_search_ad:
                    headlines = [h.text for h in ad.responsive_search_ad.headlines]
                    descriptions = [d.text for d in ad.responsive_search_ad.descriptions]

                ads.append({
                    "ad_id": str(ad.id),
                    "ad_type": ad.type_.name,
                    "status": row.ad_group_ad.status.name,
                    "ad_strength": row.ad_group_ad.ad_strength.name if row.ad_group_ad.ad_strength else None,
                    "final_urls": list(ad.final_urls) if ad.final_urls else [],
                    "headlines": headlines,
                    "descriptions": descriptions,
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "ad_group_id": str(row.ad_group.id),
                    "ad_group_name": row.ad_group.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "ctr": round(row.metrics.ctr * 100, 2) if row.metrics.ctr else 0,
                    "cost": round(row.metrics.cost_micros / 1_000_000, 2),
                    "conversions": row.metrics.conversions,
                    "conversion_value": round(row.metrics.conversions_value, 2)
                })

            return {
                "status": "success",
                "date_range": date_range,
                "ad_count": len(ads),
                "ads": ads
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)

    async def get_daily_performance(
        self,
        customer_id: str,
        campaign_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS"
    ) -> Dict[str, Any]:
        """Fetch daily performance data for trend analysis"""
        customer_id = self._format_customer_id(customer_id)
        date_range = self._validate_date_range(date_range)

        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    segments.date,
                    campaign.id,
                    campaign.name,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.ctr,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.conversions_value
                FROM campaign
                WHERE segments.date DURING {date_range}
            """

            if campaign_id:
                campaign_id = self._validate_numeric_id(campaign_id, "campaign_id")
                query += f" AND campaign.id = {campaign_id}"

            query += " ORDER BY segments.date DESC"

            response = await asyncio.to_thread(
                ga_service.search,
                customer_id=customer_id,
                query=query
            )

            daily_data = []
            for row in response:
                daily_data.append({
                    "date": row.segments.date,
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "ctr": round(row.metrics.ctr * 100, 2) if row.metrics.ctr else 0,
                    "cost": round(row.metrics.cost_micros / 1_000_000, 2),
                    "conversions": row.metrics.conversions,
                    "conversion_value": round(row.metrics.conversions_value, 2)
                })

            return {
                "status": "success",
                "date_range": date_range,
                "data_points": len(daily_data),
                "daily_performance": daily_data
            }

        except GoogleAdsException as ex:
            raise self._parse_google_ads_exception(ex)


# Global service instance
_google_ads_service: Optional[GoogleAdsService] = None


async def get_google_ads_service(config: GoogleAdsConfig) -> GoogleAdsService:
    """Get or create Google Ads service instance"""
    global _google_ads_service

    if _google_ads_service is None:
        _google_ads_service = GoogleAdsService(config)

    return _google_ads_service


async def cleanup_google_ads_service():
    """Cleanup Google Ads service resources"""
    global _google_ads_service
    _google_ads_service = None
