"""
Google Ads Tools Module
MCP tools for Google Ads API operations (v3 compatible)
"""

import logging
import json
from typing import Optional, Dict, TYPE_CHECKING

from mcp.server import Server

from mcp_esa.services.google_ads_service import (
    GoogleAdsService,
    GoogleAdsConfig,
    GoogleAdsAPIError,
    GOOGLE_ADS_AVAILABLE
)
from mcp_esa.config.settings import get_settings

if TYPE_CHECKING:
    from mcp_esa.server.mcp_server import MCPServerApp

logger = logging.getLogger(__name__)


def _format_audit_data(campaigns, keywords, quality, search_terms, account, date_range) -> str:
    """Format account data for LLM audit analysis"""

    acc = account.get('metrics', {})
    output = f"""ACCOUNT PERFORMANCE ({date_range}):
- Total Impressions: {acc.get('impressions', 0):,}
- Total Clicks: {acc.get('clicks', 0):,}
- CTR: {acc.get('ctr', 0):.2f}%
- Total Spend: ${acc.get('cost', 0):,.2f}
- Conversions: {acc.get('conversions', 0):.1f}
- Conversion Value: ${acc.get('conversion_value', 0):,.2f}

CAMPAIGNS ({campaigns.get('campaign_count', 0)} total):
"""
    for c in campaigns.get('campaigns', [])[:15]:
        if 'metrics' in c:
            m = c['metrics']
            output += f"- {c['name']} [{c['status']}]: {m['impressions']:,} impr, {m['clicks']} clicks, ${m['cost']:.2f}, {m['conversions']:.1f} conv\n"
        else:
            output += f"- {c['name']} [{c['status']}]\n"

    output += f"\n\nKEYWORDS ({keywords.get('keyword_count', 0)} analyzed):\n"
    for kw in keywords.get('keywords', [])[:20]:
        output += f"- {kw['keyword']} [{kw['match_type']}]: {kw['impressions']:,} impr, {kw['clicks']} clicks, ${kw['cost']:.2f}, {kw['conversions']:.1f} conv\n"

    output += f"\n\nQUALITY SCORES:\n"
    qs_list = quality.get('quality_scores', [])
    low_qs = [q for q in qs_list if q['quality_score'] and q['quality_score'] < 5]
    med_qs = [q for q in qs_list if q['quality_score'] and 5 <= q['quality_score'] < 7]
    high_qs = [q for q in qs_list if q['quality_score'] and q['quality_score'] >= 7]
    output += f"- High QS (7-10): {len(high_qs)} keywords\n"
    output += f"- Medium QS (5-6): {len(med_qs)} keywords\n"
    output += f"- Low QS (1-4): {len(low_qs)} keywords\n"

    if low_qs:
        output += "\nLow QS Keywords:\n"
        for q in low_qs[:10]:
            output += f"  - {q['keyword']}: QS={q['quality_score']}, CTR={q['expected_ctr']}, Ad={q['creative_quality']}, LP={q['landing_page_quality']}\n"

    output += f"\n\nSEARCH TERMS ({search_terms.get('search_term_count', 0)} analyzed):\n"
    converting = [st for st in search_terms.get('search_terms', []) if st['conversions'] > 0]
    non_converting = [st for st in search_terms.get('search_terms', []) if st['conversions'] == 0 and st['cost'] > 0]

    output += f"Converting terms ({len(converting)}):\n"
    for st in sorted(converting, key=lambda x: x['conversions'], reverse=True)[:10]:
        output += f"- \"{st['search_term']}\": {st['conversions']:.1f} conv, ${st['cost']:.2f}\n"

    output += f"\nNon-converting with spend ({len(non_converting)}):\n"
    for st in sorted(non_converting, key=lambda x: x['cost'], reverse=True)[:10]:
        output += f"- \"{st['search_term']}\": ${st['cost']:.2f}, {st['clicks']} clicks\n"

    return output


async def get_google_ads_config() -> Optional[GoogleAdsConfig]:
    """Get Google Ads configuration from mcp.yaml + vault secrets"""
    settings = get_settings()
    if not settings.google_ads_enabled:
        return None

    return GoogleAdsConfig(
        client_id=settings.google_ads_client_id,
        client_secret=settings.google_ads_client_secret,
        refresh_token=settings.google_ads_refresh_token,
        developer_token=settings.google_ads_developer_token,
        login_customer_id=settings.google_ads_login_customer_id
    )


def register_google_ads_tools(server: Server, app: 'MCPServerApp') -> None:
    """Register all Google Ads tools with the MCP server"""

    if not hasattr(server, '_tool_handlers'):
        server._tool_handlers = {}

    logger.info("Registering Google Ads tools")

    # =========================================================================
    # UTILITY TOOLS
    # =========================================================================

    async def ga_test_connection(auth_context: Optional[Dict] = None) -> str:
        """Test Google Ads API connection and return status info"""
        try:
            if not GOOGLE_ADS_AVAILABLE:
                return "Google Ads library not installed. Run: pip install google-ads>=25.0.0"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured. Set GOOGLE_ADS_ENABLED=true and configure credentials in .env"

            service = GoogleAdsService(config)
            result = await service.test_connection()

            if result['status'] == 'success':
                return (
                    f"Google Ads API Connection: SUCCESS\n\n"
                    f"Accessible Accounts: {result.get('accessible_customers', 0)}\n"
                    f"Developer Token: {result.get('developer_token_status', 'unknown')}\n"
                    f"Login Customer ID: {result.get('login_customer_id', 'not set')}"
                )
            else:
                return f"Google Ads API Connection: FAILED\n\nError: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Google Ads connection test failed: {e}", exc_info=True)
            return "Connection test failed. Check server logs for details."

    # =========================================================================
    # ACCOUNT MANAGEMENT TOOLS
    # =========================================================================

    async def ga_list_accessible_customers(auth_context: Optional[Dict] = None) -> str:
        """List all Google Ads accounts accessible with current credentials"""
        try:
            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.list_accessible_customers()

            if result['status'] == 'success':
                customer_ids = result.get('customer_ids', [])
                if not customer_ids:
                    return "No accessible Google Ads accounts found"

                output = f"Found {len(customer_ids)} accessible Google Ads account(s):\n\n"
                for cid in customer_ids:
                    # Format with dashes for readability
                    formatted_id = f"{cid[:3]}-{cid[3:6]}-{cid[6:]}" if len(cid) == 10 else cid
                    output += f"  {formatted_id}\n"

                output += "\nUse GA_get_account_info to get details for a specific account."
                return output
            else:
                return f"Failed to list customers: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error listing accessible customers: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_get_account_info(
        auth_context: Optional[Dict] = None,
        customer_id: str = None
    ) -> str:
        """Get details for a specific Google Ads account"""
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.get_account_info(customer_id)

            if result['status'] == 'success':
                account = result['account']
                return (
                    f"Google Ads Account Details\n\n"
                    f"ID: {account['id']}\n"
                    f"Name: {account['name']}\n"
                    f"Currency: {account['currency_code']}\n"
                    f"Time Zone: {account['time_zone']}\n"
                    f"Is Manager Account: {account['is_manager']}\n"
                    f"Status: {account['status']}"
                )
            else:
                return f"Failed to get account info: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error getting account info: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    # =========================================================================
    # CAMPAIGN MANAGEMENT TOOLS
    # =========================================================================

    async def ga_list_campaigns(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        include_metrics: bool = False,
        status_filter: Optional[str] = None
    ) -> str:
        """
        List campaigns for a Google Ads account

        Args:
            customer_id: Google Ads customer ID (required)
            include_metrics: Include performance metrics (impressions, clicks, cost, conversions)
            status_filter: Filter by status (ENABLED, PAUSED, REMOVED)
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.list_campaigns(
                customer_id=customer_id,
                include_metrics=include_metrics,
                status_filter=status_filter
            )

            if result['status'] == 'success':
                campaigns = result.get('campaigns', [])
                if not campaigns:
                    return f"No campaigns found for account {customer_id}"

                output = f"Found {len(campaigns)} campaign(s):\n\n"
                for c in campaigns:
                    output += f"Campaign: {c['name']}\n"
                    output += f"  ID: {c['id']}\n"
                    output += f"  Status: {c['status']}\n"
                    output += f"  Channel: {c['channel_type']}\n"

                    if include_metrics and 'metrics' in c:
                        m = c['metrics']
                        output += f"  Impressions: {m['impressions']:,}\n"
                        output += f"  Clicks: {m['clicks']:,}\n"
                        output += f"  Cost: ${m['cost']:.2f}\n"
                        output += f"  Conversions: {m['conversions']:.1f}\n"

                    output += "\n"

                return output
            else:
                return f"Failed to list campaigns: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error listing campaigns: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_get_campaign(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: str = None
    ) -> str:
        """Get details for a specific campaign including metrics"""
        try:
            if not customer_id or not campaign_id:
                return "customer_id and campaign_id are required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.get_campaign(customer_id, campaign_id)

            if result['status'] == 'success':
                c = result['campaign']
                m = c['metrics']
                return (
                    f"Campaign Details\n\n"
                    f"Name: {c['name']}\n"
                    f"ID: {c['id']}\n"
                    f"Status: {c['status']}\n"
                    f"Channel Type: {c['channel_type']}\n"
                    f"Channel Sub-Type: {c['channel_sub_type']}\n"
                    f"Bidding Strategy: {c['bidding_strategy_type']}\n"
                    f"Daily Budget: ${c['budget']:.2f}\n\n"
                    f"Performance Metrics:\n"
                    f"  Impressions: {m['impressions']:,}\n"
                    f"  Clicks: {m['clicks']:,}\n"
                    f"  CTR: {m['ctr']:.2%}\n"
                    f"  Cost: ${m['cost']:.2f}\n"
                    f"  Avg CPC: ${m['average_cpc']:.2f}\n"
                    f"  Conversions: {m['conversions']:.1f}"
                )
            else:
                return f"Failed to get campaign: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error getting campaign: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_create_campaign(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        name: str = None,
        budget_amount: float = None,
        advertising_channel_type: str = "SEARCH",
        status: str = "PAUSED"
    ) -> str:
        """
        Create a new campaign

        Args:
            customer_id: Google Ads customer ID
            name: Campaign name
            budget_amount: Daily budget amount in account currency
            advertising_channel_type: SEARCH, DISPLAY, SHOPPING, VIDEO, etc.
            status: Initial status (ENABLED, PAUSED)
        """
        try:
            if not all([customer_id, name, budget_amount]):
                return "customer_id, name, and budget_amount are required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.create_campaign(
                customer_id=customer_id,
                name=name,
                budget_amount=budget_amount,
                advertising_channel_type=advertising_channel_type,
                status=status
            )

            if result['status'] == 'success':
                return (
                    f"Campaign created successfully!\n\n"
                    f"Campaign Name: {name}\n"
                    f"Campaign ID: {result['campaign_id']}\n"
                    f"Daily Budget: ${budget_amount:.2f}\n"
                    f"Status: {status}\n"
                    f"Channel: {advertising_channel_type}"
                )
            else:
                return f"Failed to create campaign: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error creating campaign: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_update_campaign(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: str = None,
        name: Optional[str] = None,
        status: Optional[str] = None,
        budget_amount: Optional[float] = None
    ) -> str:
        """
        Update campaign settings

        Args:
            customer_id: Google Ads customer ID
            campaign_id: Campaign to update
            name: New campaign name (optional)
            status: New status - ENABLED, PAUSED (optional)
            budget_amount: New daily budget (optional)
        """
        try:
            if not customer_id or not campaign_id:
                return "customer_id and campaign_id are required"

            if not any([name, status, budget_amount]):
                return "At least one of name, status, or budget_amount must be provided"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.update_campaign(
                customer_id=customer_id,
                campaign_id=campaign_id,
                name=name,
                status=status,
                budget_amount=budget_amount
            )

            if result['status'] == 'success':
                updates = []
                if name:
                    updates.append(f"Name: {name}")
                if status:
                    updates.append(f"Status: {status}")
                if budget_amount:
                    updates.append(f"Budget: ${budget_amount:.2f}")

                return f"Campaign {campaign_id} updated successfully!\n\nUpdates:\n" + "\n".join(f"  {u}" for u in updates)
            else:
                return f"Failed to update campaign: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error updating campaign: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_set_campaign_status(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: str = None,
        status: str = None
    ) -> str:
        """
        Change campaign status

        Args:
            customer_id: Google Ads customer ID
            campaign_id: Campaign to update
            status: New status - ENABLED, PAUSED, or REMOVED
        """
        try:
            if not all([customer_id, campaign_id, status]):
                return "customer_id, campaign_id, and status are required"

            valid_statuses = ['ENABLED', 'PAUSED', 'REMOVED']
            if status.upper() not in valid_statuses:
                return f"status must be one of: {', '.join(valid_statuses)}"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.set_campaign_status(
                customer_id=customer_id,
                campaign_id=campaign_id,
                status=status.upper()
            )

            if result['status'] == 'success':
                return f"Campaign {campaign_id} status changed to {status.upper()}"
            else:
                return f"Failed to update status: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error setting campaign status: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    # =========================================================================
    # AD GROUP MANAGEMENT TOOLS
    # =========================================================================

    async def ga_list_ad_groups(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: Optional[str] = None
    ) -> str:
        """
        List ad groups, optionally filtered by campaign

        Args:
            customer_id: Google Ads customer ID (required)
            campaign_id: Filter by specific campaign (optional)
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.list_ad_groups(
                customer_id=customer_id,
                campaign_id=campaign_id
            )

            if result['status'] == 'success':
                ad_groups = result.get('ad_groups', [])
                if not ad_groups:
                    filter_msg = f" for campaign {campaign_id}" if campaign_id else ""
                    return f"No ad groups found{filter_msg}"

                output = f"Found {len(ad_groups)} ad group(s):\n\n"
                for ag in ad_groups:
                    m = ag['metrics']
                    output += f"Ad Group: {ag['name']}\n"
                    output += f"  ID: {ag['id']}\n"
                    output += f"  Status: {ag['status']}\n"
                    output += f"  Type: {ag['type']}\n"
                    output += f"  Campaign: {ag['campaign_name']}\n"
                    output += f"  Impressions: {m['impressions']:,}\n"
                    output += f"  Clicks: {m['clicks']:,}\n"
                    output += f"  Cost: ${m['cost']:.2f}\n\n"

                return output
            else:
                return f"Failed to list ad groups: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error listing ad groups: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_create_ad_group(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: str = None,
        name: str = None,
        cpc_bid: float = 1.00,
        status: str = "ENABLED"
    ) -> str:
        """
        Create a new ad group

        Args:
            customer_id: Google Ads customer ID
            campaign_id: Campaign to add ad group to
            name: Ad group name
            cpc_bid: Default CPC bid in account currency (default: 1.00)
            status: Initial status (ENABLED, PAUSED)
        """
        try:
            if not all([customer_id, campaign_id, name]):
                return "customer_id, campaign_id, and name are required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.create_ad_group(
                customer_id=customer_id,
                campaign_id=campaign_id,
                name=name,
                cpc_bid_micros=int(cpc_bid * 1_000_000),
                status=status.upper()
            )

            if result['status'] == 'success':
                return (
                    f"Ad group created successfully!\n\n"
                    f"Ad Group Name: {name}\n"
                    f"Ad Group ID: {result['ad_group_id']}\n"
                    f"Default CPC Bid: ${cpc_bid:.2f}\n"
                    f"Status: {status}"
                )
            else:
                return f"Failed to create ad group: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error creating ad group: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_update_ad_group(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        ad_group_id: str = None,
        name: Optional[str] = None,
        status: Optional[str] = None,
        cpc_bid: Optional[float] = None
    ) -> str:
        """
        Update ad group settings

        Args:
            customer_id: Google Ads customer ID
            ad_group_id: Ad group to update
            name: New ad group name (optional)
            status: New status - ENABLED, PAUSED (optional)
            cpc_bid: New default CPC bid (optional)
        """
        try:
            if not customer_id or not ad_group_id:
                return "customer_id and ad_group_id are required"

            if not any([name, status, cpc_bid]):
                return "At least one of name, status, or cpc_bid must be provided"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.update_ad_group(
                customer_id=customer_id,
                ad_group_id=ad_group_id,
                name=name,
                status=status.upper() if status else None,
                cpc_bid_micros=int(cpc_bid * 1_000_000) if cpc_bid else None
            )

            if result['status'] == 'success':
                updates = []
                if name:
                    updates.append(f"Name: {name}")
                if status:
                    updates.append(f"Status: {status}")
                if cpc_bid:
                    updates.append(f"CPC Bid: ${cpc_bid:.2f}")

                return f"Ad group {ad_group_id} updated successfully!\n\nUpdates:\n" + "\n".join(f"  {u}" for u in updates)
            else:
                return f"Failed to update ad group: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error updating ad group: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    # =========================================================================
    # REPORTING & ANALYTICS TOOLS
    # =========================================================================

    async def ga_query(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        query: str = None
    ) -> str:
        """
        Execute a Google Ads Query Language (GAQL) query

        This is the most powerful tool - allows executing any GAQL query.

        Args:
            customer_id: Google Ads customer ID
            query: GAQL query string

        Example queries:
            SELECT campaign.id, campaign.name, campaign.status FROM campaign LIMIT 10

            SELECT ad_group.id, ad_group.name, metrics.clicks
            FROM ad_group
            WHERE segments.date DURING LAST_30_DAYS
        """
        try:
            if not customer_id or not query:
                return "customer_id and query are required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.execute_query(customer_id, query)

            if result['status'] == 'success':
                row_count = result.get('row_count', 0)
                results = result.get('results', [])

                if row_count == 0:
                    return "Query executed successfully. No results returned."

                # Format as JSON for complex results
                output = f"Query returned {row_count} row(s):\n\n"
                output += json.dumps(results, indent=2, default=str)
                return output
            else:
                return f"Query failed: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error executing query: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_get_campaign_performance(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        date_range: str = "LAST_30_DAYS",
        campaign_id: Optional[str] = None
    ) -> str:
        """
        Get campaign performance metrics

        Args:
            customer_id: Google Ads customer ID
            date_range: Date range for metrics (LAST_7_DAYS, LAST_30_DAYS, LAST_90_DAYS, THIS_MONTH, etc.)
            campaign_id: Filter to specific campaign (optional)
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.get_campaign_performance(
                customer_id=customer_id,
                date_range=date_range,
                campaign_id=campaign_id
            )

            if result['status'] == 'success':
                campaigns = result.get('campaigns', [])
                if not campaigns:
                    return f"No campaign data for {date_range}"

                output = f"Campaign Performance ({date_range})\n"
                output += "=" * 50 + "\n\n"

                for c in campaigns:
                    output += f"Campaign: {c['campaign_name']}\n"
                    output += f"  Status: {c['status']}\n"
                    output += f"  Impressions: {c['impressions']:,}\n"
                    output += f"  Clicks: {c['clicks']:,}\n"
                    output += f"  CTR: {c['ctr']:.2f}%\n"
                    output += f"  Cost: ${c['cost']:,.2f}\n"
                    output += f"  Avg CPC: ${c['average_cpc']:.2f}\n"
                    output += f"  Conversions: {c['conversions']:.1f}\n"
                    output += f"  Conv Value: ${c['conversion_value']:,.2f}\n"
                    output += f"  Cost/Conv: ${c['cost_per_conversion']:.2f}\n\n"

                return output
            else:
                return f"Failed to get performance: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error getting campaign performance: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_get_account_performance(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        date_range: str = "LAST_30_DAYS"
    ) -> str:
        """
        Get account-level performance summary

        Args:
            customer_id: Google Ads customer ID
            date_range: Date range for metrics
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.get_account_performance(
                customer_id=customer_id,
                date_range=date_range
            )

            if result['status'] == 'success':
                account = result.get('account', {})
                m = result.get('metrics', {})

                return (
                    f"Account Performance Summary ({date_range})\n"
                    f"{'=' * 50}\n\n"
                    f"Account: {account.get('name', 'N/A')}\n"
                    f"ID: {account.get('id', 'N/A')}\n"
                    f"Currency: {account.get('currency', 'N/A')}\n\n"
                    f"Metrics:\n"
                    f"  Impressions: {m.get('impressions', 0):,}\n"
                    f"  Clicks: {m.get('clicks', 0):,}\n"
                    f"  CTR: {m.get('ctr', 0):.2f}%\n"
                    f"  Total Cost: ${m.get('cost', 0):,.2f}\n"
                    f"  Conversions: {m.get('conversions', 0):.1f}\n"
                    f"  Conversion Value: ${m.get('conversion_value', 0):,.2f}"
                )
            else:
                return f"Failed to get account performance: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error getting account performance: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_get_keyword_performance(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: Optional[str] = None,
        ad_group_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS"
    ) -> str:
        """
        Get keyword-level performance metrics

        Args:
            customer_id: Google Ads customer ID
            campaign_id: Filter by campaign (optional)
            ad_group_id: Filter by ad group (optional)
            date_range: Date range for metrics
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)
            result = await service.get_keyword_performance(
                customer_id=customer_id,
                campaign_id=campaign_id,
                ad_group_id=ad_group_id,
                date_range=date_range
            )

            if result['status'] == 'success':
                keywords = result.get('keywords', [])
                if not keywords:
                    return f"No keyword data for {date_range}"

                output = f"Keyword Performance ({date_range})\n"
                output += f"Top {len(keywords)} keywords by impressions:\n"
                output += "=" * 60 + "\n\n"

                for kw in keywords:
                    output += f"Keyword: {kw['keyword']} [{kw['match_type']}]\n"
                    output += f"  Campaign: {kw['campaign_name']}\n"
                    output += f"  Ad Group: {kw['ad_group_name']}\n"
                    output += f"  Status: {kw['status']}\n"
                    output += f"  Impressions: {kw['impressions']:,}\n"
                    output += f"  Clicks: {kw['clicks']:,}\n"
                    output += f"  CTR: {kw['ctr']:.2f}%\n"
                    output += f"  Cost: ${kw['cost']:.2f}\n"
                    output += f"  Avg CPC: ${kw['average_cpc']:.2f}\n"
                    output += f"  Conversions: {kw['conversions']:.1f}\n\n"

                return output
            else:
                return f"Failed to get keyword performance: {result.get('message', 'Unknown error')}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error getting keyword performance: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    # =========================================================================
    # AI-POWERED ANALYSIS TOOLS
    # =========================================================================

    async def ga_audit_account(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        date_range: str = "LAST_30_DAYS"
    ) -> str:
        """
        Comprehensive AI-powered Google Ads account audit.

        Analyzes:
        - Campaign structure and performance
        - Keyword health and quality scores
        - Budget utilization
        - Conversion efficiency
        - Competitive positioning

        Returns actionable recommendations prioritized by impact.

        Args:
            customer_id: Google Ads customer ID
            date_range: Date range for analysis (LAST_7_DAYS, LAST_30_DAYS, LAST_90_DAYS)
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)

            # Fetch all required data in parallel
            campaigns_task = service.list_campaigns(customer_id, include_metrics=True)
            keywords_task = service.get_keyword_performance(customer_id, date_range=date_range)
            quality_task = service.get_quality_scores(customer_id)
            search_terms_task = service.get_search_terms(customer_id, date_range=date_range, limit=200)
            account_task = service.get_account_performance(customer_id, date_range=date_range)

            import asyncio
            campaigns, keywords, quality, search_terms, account = await asyncio.gather(
                campaigns_task, keywords_task, quality_task, search_terms_task, account_task
            )

            # Prepare data summary for LLM
            data_summary = _format_audit_data(campaigns, keywords, quality, search_terms, account, date_range)

            # Get LLM analysis
            from services.llm.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM service unavailable. Raw data summary:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError as e:
                return f"No chat provider configured: {e}\n\nRaw data summary:\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": """You are an expert Google Ads auditor and PPC specialist. Analyze the account data
and provide a structured audit report with:

1. **Executive Summary** (2-3 sentences on overall account health)
2. **Performance Scorecard** (rate each area 1-10):
   - Campaign Structure
   - Keyword Quality
   - Budget Efficiency
   - Conversion Performance
3. **Key Findings** (top 5 issues/opportunities with specific data)
4. **Prioritized Recommendations** (High/Medium/Low impact with specific actions)
5. **Quick Wins** (changes that can be made immediately)

Be specific with numbers, percentages, and actionable recommendations. Focus on ROI impact."""
                },
                {
                    "role": "user",
                    "content": f"Audit this Google Ads account:\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=3000
            )

            return f"Google Ads Account Audit ({date_range})\n{'='*50}\n\n{response['content']}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error in account audit: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_analyze_keywords(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS"
    ) -> str:
        """
        AI-powered keyword strategy analysis.

        Identifies:
        - Top performers to scale
        - Underperformers to optimize or pause
        - Keyword cannibalization issues
        - Match type optimization opportunities
        - Missing keyword opportunities from search terms

        Args:
            customer_id: Google Ads customer ID
            campaign_id: Optional campaign filter
            date_range: Date range for analysis
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)

            # Fetch keyword and search term data
            import asyncio
            keywords, search_terms, quality = await asyncio.gather(
                service.get_keyword_performance(customer_id, campaign_id=campaign_id, date_range=date_range),
                service.get_search_terms(customer_id, campaign_id=campaign_id, date_range=date_range, limit=300),
                service.get_quality_scores(customer_id, campaign_id=campaign_id)
            )

            # Prepare analysis data
            data_summary = f"""KEYWORD PERFORMANCE ({date_range}):
Keywords analyzed: {keywords.get('keyword_count', 0)}

Top Keywords by Impressions:
"""
            for kw in keywords.get('keywords', [])[:20]:
                data_summary += f"- {kw['keyword']} [{kw['match_type']}]: {kw['impressions']:,} impr, {kw['clicks']} clicks, ${kw['cost']:.2f}, {kw['conversions']:.1f} conv\n"

            data_summary += f"\n\nQUALITY SCORES:\n"
            for qs in quality.get('quality_scores', [])[:15]:
                if qs['quality_score']:
                    data_summary += f"- {qs['keyword']}: QS={qs['quality_score']}, CTR={qs['expected_ctr']}, Ad={qs['creative_quality']}, LP={qs['landing_page_quality']}\n"

            data_summary += f"\n\nSEARCH TERMS (converting & high volume):\n"
            for st in search_terms.get('search_terms', [])[:30]:
                data_summary += f"- \"{st['search_term']}\": {st['impressions']:,} impr, {st['clicks']} clicks, {st['conversions']:.1f} conv, ${st['cost']:.2f}\n"

            # Get LLM analysis
            from services.llm.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw data:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": """You are a Google Ads keyword strategy expert. Analyze this data and provide:

1. **Top Performers** (keywords to scale with increased bids/budget)
2. **Underperformers** (keywords to pause or optimize, with reasons)
3. **Cannibalization Issues** (keywords competing against each other)
4. **Match Type Recommendations** (broad vs phrase vs exact opportunities)
5. **New Keyword Opportunities** (from search terms not yet targeted)
6. **Quality Score Issues** (keywords with low QS and how to fix)

Be specific with keyword names and actionable recommendations."""
                },
                {
                    "role": "user",
                    "content": f"Analyze this keyword data:\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=2500
            )

            return f"Keyword Strategy Analysis ({date_range})\n{'='*50}\n\n{response['content']}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error in keyword analysis: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_analyze_search_terms(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS"
    ) -> str:
        """
        AI-powered search term analysis.

        Analyzes:
        - Search intent patterns (informational, transactional, navigational)
        - Wasted spend on irrelevant queries
        - High-converting terms not yet targeted
        - Query-to-keyword alignment issues

        Args:
            customer_id: Google Ads customer ID
            campaign_id: Optional campaign filter
            date_range: Date range for analysis
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)

            # Fetch search terms and negative keywords
            import asyncio
            search_terms, negatives = await asyncio.gather(
                service.get_search_terms(customer_id, campaign_id=campaign_id, date_range=date_range, limit=500),
                service.get_negative_keywords(customer_id, campaign_id=campaign_id)
            )

            # Prepare data
            data_summary = f"""SEARCH TERMS ({date_range}):
Total search terms: {search_terms.get('search_term_count', 0)}

HIGH SPEND SEARCH TERMS (by cost):
"""
            sorted_by_cost = sorted(search_terms.get('search_terms', []), key=lambda x: x['cost'], reverse=True)
            for st in sorted_by_cost[:30]:
                data_summary += f"- \"{st['search_term']}\": ${st['cost']:.2f}, {st['clicks']} clicks, {st['conversions']:.1f} conv\n"

            data_summary += f"\n\nNON-CONVERTING SEARCH TERMS (high spend, 0 conversions):\n"
            non_converting = [st for st in search_terms.get('search_terms', []) if st['conversions'] == 0 and st['cost'] > 1]
            for st in sorted(non_converting, key=lambda x: x['cost'], reverse=True)[:20]:
                data_summary += f"- \"{st['search_term']}\": ${st['cost']:.2f}, {st['clicks']} clicks\n"

            data_summary += f"\n\nCONVERTING SEARCH TERMS:\n"
            converting = [st for st in search_terms.get('search_terms', []) if st['conversions'] > 0]
            for st in sorted(converting, key=lambda x: x['conversions'], reverse=True)[:20]:
                cpa = st['cost'] / st['conversions'] if st['conversions'] > 0 else 0
                data_summary += f"- \"{st['search_term']}\": {st['conversions']:.1f} conv, ${cpa:.2f} CPA, ${st['cost']:.2f} spend\n"

            data_summary += f"\n\nEXISTING NEGATIVE KEYWORDS ({len(negatives.get('negative_keywords', []))}):\n"
            for neg in negatives.get('negative_keywords', [])[:20]:
                data_summary += f"- {neg['keyword']} [{neg['match_type']}]\n"

            # Get LLM analysis
            from services.llm.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw data:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": """You are a search query analyst for Google Ads. Analyze this data and provide:

1. **Intent Analysis** (categorize search patterns: transactional, informational, navigational, competitor)
2. **Wasted Spend** (irrelevant queries costing money - be specific)
3. **New Keyword Opportunities** (converting searches that should be added as keywords)
4. **Negative Keyword Recommendations** (specific terms to exclude with match types)
5. **Query-Keyword Alignment** (mismatches between user intent and triggered ads)

Focus on ROI impact and provide specific search terms in your recommendations."""
                },
                {
                    "role": "user",
                    "content": f"Analyze these search terms:\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=2500
            )

            return f"Search Term Analysis ({date_range})\n{'='*50}\n\n{response['content']}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error in search term analysis: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_suggest_negative_keywords(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS"
    ) -> str:
        """
        AI-powered negative keyword suggestions based on search term analysis.

        Identifies irrelevant, low-intent, and wasteful search queries
        and recommends specific negative keywords with match types.

        Args:
            customer_id: Google Ads customer ID
            campaign_id: Optional campaign filter
            date_range: Date range for analysis
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)

            import asyncio
            search_terms, negatives = await asyncio.gather(
                service.get_search_terms(customer_id, campaign_id=campaign_id, date_range=date_range, limit=500),
                service.get_negative_keywords(customer_id, campaign_id=campaign_id)
            )

            # Focus on non-converting and low-converting terms
            data_summary = "SEARCH TERMS WITH POOR PERFORMANCE:\n\n"

            non_converting = [st for st in search_terms.get('search_terms', []) if st['conversions'] == 0 and st['clicks'] > 0]
            data_summary += "Zero Conversions (with clicks):\n"
            for st in sorted(non_converting, key=lambda x: x['cost'], reverse=True)[:40]:
                data_summary += f"- \"{st['search_term']}\": ${st['cost']:.2f}, {st['clicks']} clicks, {st['impressions']:,} impr\n"

            low_ctr = [st for st in search_terms.get('search_terms', []) if st['ctr'] < 1 and st['impressions'] > 50]
            data_summary += f"\n\nLow CTR (<1%) with 50+ impressions:\n"
            for st in sorted(low_ctr, key=lambda x: x['impressions'], reverse=True)[:20]:
                data_summary += f"- \"{st['search_term']}\": {st['ctr']:.2f}% CTR, {st['impressions']:,} impr\n"

            data_summary += f"\n\nEXISTING NEGATIVES ({len(negatives.get('negative_keywords', []))}):\n"
            for neg in negatives.get('negative_keywords', [])[:15]:
                data_summary += f"- {neg['keyword']} [{neg['match_type']}]\n"

            # Get LLM analysis
            from services.llm.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw data:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": """You are a negative keyword specialist. Based on the search term data, provide:

1. **Immediate Additions** (high-priority negatives to add now)
   - Format: keyword [EXACT/PHRASE/BROAD] - reason

2. **Pattern-Based Negatives** (recurring irrelevant themes)
   - Identify common words/phrases appearing in bad queries

3. **Low-Intent Terms** (informational searches unlikely to convert)

4. **Competitor/Brand Terms** (if appearing inappropriately)

5. **Implementation Priority** (rank by potential savings)

Be specific with exact terms and recommended match types. Consider the existing negatives to avoid duplicates."""
                },
                {
                    "role": "user",
                    "content": f"Suggest negative keywords based on:\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.2,
                max_tokens=2000
            )

            return f"Negative Keyword Recommendations ({date_range})\n{'='*50}\n\n{response['content']}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error suggesting negatives: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_analyze_competitors(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS"
    ) -> str:
        """
        AI-powered competitor analysis from auction insights.

        Analyzes:
        - Main competitors and their positioning
        - Impression share opportunities
        - Where you're winning vs losing
        - Bidding strategy recommendations

        Args:
            customer_id: Google Ads customer ID
            campaign_id: Optional campaign filter
            date_range: Date range for analysis
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)

            import asyncio
            auction, campaigns = await asyncio.gather(
                service.get_auction_insights(customer_id, campaign_id=campaign_id, date_range=date_range),
                service.get_campaign_performance(customer_id, date_range=date_range, campaign_id=campaign_id)
            )

            data_summary = f"""AUCTION INSIGHTS ({date_range}):
Competitors analyzed: {auction.get('competitor_count', 0)}

COMPETITOR METRICS:
"""
            for insight in auction.get('auction_insights', []):
                data_summary += f"""
{insight['domain']}:
  - Impression Share: {insight['impression_share']}%
  - Overlap Rate: {insight['overlap_rate']}%
  - Position Above Rate: {insight['position_above_rate']}%
  - Top Impression %: {insight['top_impression_pct']}%
  - Abs Top Impression %: {insight['abs_top_impression_pct']}%
  - Outranking Share: {insight['outranking_share']}%
"""

            data_summary += f"\n\nYOUR CAMPAIGN PERFORMANCE:\n"
            for c in campaigns.get('campaigns', [])[:5]:
                data_summary += f"- {c['campaign_name']}: {c['impressions']:,} impr, ${c['cost']:,.2f}, {c['conversions']:.1f} conv\n"

            # Get LLM analysis
            from services.llm.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw data:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": """You are a competitive intelligence analyst for Google Ads. Analyze this data and provide:

1. **Competitive Landscape** (who are the main players, their apparent strategies)
2. **Your Position** (where you stand vs competitors)
3. **Opportunities** (where you can gain impression share)
4. **Threats** (competitors outperforming you significantly)
5. **Bidding Recommendations** (when to bid more aggressively, when to pull back)
6. **Strategic Insights** (patterns in competitor behavior)

Be specific with competitor names and actionable recommendations."""
                },
                {
                    "role": "user",
                    "content": f"Analyze this competitive data:\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=2000
            )

            return f"Competitive Analysis ({date_range})\n{'='*50}\n\n{response['content']}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error in competitor analysis: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_analyze_quality_scores(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: Optional[str] = None
    ) -> str:
        """
        AI-powered quality score analysis and optimization recommendations.

        Identifies:
        - Keywords with low quality scores
        - Component-level issues (CTR, ad relevance, landing page)
        - Priority fixes based on impact
        - Specific optimization actions

        Args:
            customer_id: Google Ads customer ID
            campaign_id: Optional campaign filter
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)

            import asyncio
            quality, ads = await asyncio.gather(
                service.get_quality_scores(customer_id, campaign_id=campaign_id),
                service.get_ad_performance(customer_id, campaign_id=campaign_id, limit=50)
            )

            # Categorize by quality score
            qs_data = quality.get('quality_scores', [])
            low_qs = [q for q in qs_data if q['quality_score'] and q['quality_score'] < 5]
            med_qs = [q for q in qs_data if q['quality_score'] and 5 <= q['quality_score'] < 7]
            high_qs = [q for q in qs_data if q['quality_score'] and q['quality_score'] >= 7]

            data_summary = f"""QUALITY SCORE DISTRIBUTION:
- High (7-10): {len(high_qs)} keywords
- Medium (5-6): {len(med_qs)} keywords
- Low (1-4): {len(low_qs)} keywords

LOW QUALITY SCORE KEYWORDS (Priority):
"""
            for q in sorted(low_qs, key=lambda x: x['cost'], reverse=True)[:20]:
                data_summary += f"""
{q['keyword']} [QS: {q['quality_score']}]
  - Expected CTR: {q['expected_ctr']}
  - Ad Relevance: {q['creative_quality']}
  - Landing Page: {q['landing_page_quality']}
  - Cost: ${q['cost']:.2f}, Impressions: {q['impressions']:,}
"""

            data_summary += f"\n\nMEDIUM QS KEYWORDS (Quick wins):\n"
            for q in sorted(med_qs, key=lambda x: x['cost'], reverse=True)[:10]:
                data_summary += f"- {q['keyword']} [QS: {q['quality_score']}]: CTR={q['expected_ctr']}, Ad={q['creative_quality']}, LP={q['landing_page_quality']}\n"

            data_summary += f"\n\nAD PERFORMANCE (for relevance context):\n"
            for ad in ads.get('ads', [])[:10]:
                strength = ad.get('ad_strength', 'N/A')
                data_summary += f"- {ad['ad_group_name']}: strength={strength}, {ad['clicks']} clicks, {ad['ctr']:.2f}% CTR\n"

            # Get LLM analysis
            from services.llm.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw data:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": """You are a Quality Score optimization expert. Analyze this data and provide:

1. **Critical Issues** (keywords that need immediate attention)
2. **Component Analysis**:
   - Expected CTR issues and fixes
   - Ad Relevance issues and fixes
   - Landing Page issues and fixes
3. **Quick Wins** (medium QS keywords easy to improve)
4. **Ad Copy Recommendations** (based on relevance scores)
5. **Landing Page Suggestions** (based on LP scores)
6. **Prioritized Action Plan** (ordered by potential CPC savings)

Remember: Each QS point can reduce CPC by ~16%. Focus on high-spend, low-QS keywords first."""
                },
                {
                    "role": "user",
                    "content": f"Analyze these quality scores:\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=2500
            )

            return f"Quality Score Analysis\n{'='*50}\n\n{response['content']}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error in QS analysis: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_analyze_trends(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS"
    ) -> str:
        """
        AI-powered performance trend analysis with insights and forecasting.

        Analyzes:
        - Daily/weekly performance patterns
        - Trend direction (improving/declining)
        - Anomalies and their causes
        - Seasonality patterns
        - Performance forecasting

        Args:
            customer_id: Google Ads customer ID
            campaign_id: Optional campaign filter
            date_range: Date range for analysis (LAST_30_DAYS recommended for trends)
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)

            import asyncio
            daily, device = await asyncio.gather(
                service.get_daily_performance(customer_id, campaign_id=campaign_id, date_range=date_range),
                service.get_device_performance(customer_id, campaign_id=campaign_id, date_range=date_range)
            )

            # Aggregate daily data
            daily_data = daily.get('daily_performance', [])
            daily_agg = {}
            for d in daily_data:
                date = d['date']
                if date not in daily_agg:
                    daily_agg[date] = {'impressions': 0, 'clicks': 0, 'cost': 0, 'conversions': 0}
                daily_agg[date]['impressions'] += d['impressions']
                daily_agg[date]['clicks'] += d['clicks']
                daily_agg[date]['cost'] += d['cost']
                daily_agg[date]['conversions'] += d['conversions']

            data_summary = f"""DAILY PERFORMANCE TREND ({date_range}):

"""
            for date in sorted(daily_agg.keys()):
                d = daily_agg[date]
                ctr = (d['clicks'] / d['impressions'] * 100) if d['impressions'] > 0 else 0
                cpc = (d['cost'] / d['clicks']) if d['clicks'] > 0 else 0
                data_summary += f"{date}: {d['impressions']:,} impr, {d['clicks']} clicks, {ctr:.2f}% CTR, ${d['cost']:.2f}, {d['conversions']:.1f} conv\n"

            # Device breakdown
            device_agg = {}
            for d in device.get('device_breakdown', []):
                dev = d['device']
                if dev not in device_agg:
                    device_agg[dev] = {'impressions': 0, 'clicks': 0, 'cost': 0, 'conversions': 0}
                device_agg[dev]['impressions'] += d['impressions']
                device_agg[dev]['clicks'] += d['clicks']
                device_agg[dev]['cost'] += d['cost']
                device_agg[dev]['conversions'] += d['conversions']

            data_summary += f"\n\nDEVICE PERFORMANCE:\n"
            for dev, metrics in device_agg.items():
                ctr = (metrics['clicks'] / metrics['impressions'] * 100) if metrics['impressions'] > 0 else 0
                data_summary += f"- {dev}: {metrics['impressions']:,} impr, {ctr:.2f}% CTR, ${metrics['cost']:.2f}, {metrics['conversions']:.1f} conv\n"

            # Get LLM analysis
            from services.llm.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw data:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": """You are a performance analytics expert for Google Ads. Analyze this data and provide:

1. **Trend Summary** (overall direction - improving, stable, declining)
2. **Key Patterns**:
   - Day-of-week patterns
   - Week-over-week changes
   - Any anomalies or spikes
3. **Device Insights** (which devices are performing best/worst)
4. **Concerns** (declining metrics that need attention)
5. **Opportunities** (positive trends to capitalize on)
6. **Forecast** (expected performance if trends continue)
7. **Recommendations** (specific actions based on trends)

Use specific numbers and percentages in your analysis."""
                },
                {
                    "role": "user",
                    "content": f"Analyze these performance trends:\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=2000
            )

            return f"Performance Trend Analysis ({date_range})\n{'='*50}\n\n{response['content']}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error in trend analysis: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_analyze_audiences(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        campaign_id: Optional[str] = None,
        date_range: str = "LAST_30_DAYS"
    ) -> str:
        """
        AI-powered audience and targeting analysis.

        Analyzes:
        - Geographic performance patterns
        - Device targeting efficiency
        - Audience segment performance
        - Targeting recommendations

        Args:
            customer_id: Google Ads customer ID
            campaign_id: Optional campaign filter
            date_range: Date range for analysis
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)

            import asyncio
            geo, device = await asyncio.gather(
                service.get_geographic_performance(customer_id, campaign_id=campaign_id, date_range=date_range),
                service.get_device_performance(customer_id, campaign_id=campaign_id, date_range=date_range)
            )

            data_summary = f"""GEOGRAPHIC PERFORMANCE ({date_range}):
Locations: {geo.get('location_count', 0)}

Top Locations by Spend:
"""
            for loc in geo.get('geographic_data', [])[:20]:
                conv_rate = (loc['conversions'] / loc['clicks'] * 100) if loc['clicks'] > 0 else 0
                data_summary += f"- Location {loc['country_criterion_id']} ({loc['location_type']}): ${loc['cost']:.2f}, {loc['clicks']} clicks, {loc['conversions']:.1f} conv ({conv_rate:.1f}% rate)\n"

            # Aggregate device data
            device_agg = {}
            for d in device.get('device_breakdown', []):
                dev = d['device']
                if dev not in device_agg:
                    device_agg[dev] = {'impressions': 0, 'clicks': 0, 'cost': 0, 'conversions': 0, 'conv_value': 0}
                device_agg[dev]['impressions'] += d['impressions']
                device_agg[dev]['clicks'] += d['clicks']
                device_agg[dev]['cost'] += d['cost']
                device_agg[dev]['conversions'] += d['conversions']
                device_agg[dev]['conv_value'] += d['conversion_value']

            data_summary += f"\n\nDEVICE BREAKDOWN:\n"
            for dev, m in device_agg.items():
                ctr = (m['clicks'] / m['impressions'] * 100) if m['impressions'] > 0 else 0
                conv_rate = (m['conversions'] / m['clicks'] * 100) if m['clicks'] > 0 else 0
                cpa = (m['cost'] / m['conversions']) if m['conversions'] > 0 else 0
                data_summary += f"""
{dev}:
  - Impressions: {m['impressions']:,}
  - Clicks: {m['clicks']:,} ({ctr:.2f}% CTR)
  - Cost: ${m['cost']:.2f}
  - Conversions: {m['conversions']:.1f} ({conv_rate:.1f}% rate)
  - CPA: ${cpa:.2f}
  - Conv Value: ${m['conv_value']:.2f}
"""

            # Get LLM analysis
            from services.llm.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw data:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": """You are a targeting and audience expert for Google Ads. Analyze this data and provide:

1. **Geographic Insights**:
   - Best performing locations
   - Underperforming locations
   - Location bid adjustment recommendations

2. **Device Strategy**:
   - Device performance comparison
   - Device bid adjustment recommendations
   - Mobile vs Desktop optimization

3. **Targeting Opportunities**:
   - Locations to expand
   - Locations to exclude or reduce bids
   - Device-specific strategies

4. **Budget Allocation** (how to distribute budget across segments)

5. **Action Items** (specific targeting changes to make)

Be specific with location IDs and recommended bid adjustments (e.g., +20%, -30%)."""
                },
                {
                    "role": "user",
                    "content": f"Analyze this audience data:\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=2000
            )

            return f"Audience & Targeting Analysis ({date_range})\n{'='*50}\n\n{response['content']}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error in audience analysis: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_optimize_budget(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        date_range: str = "LAST_30_DAYS"
    ) -> str:
        """
        AI-powered budget optimization recommendations.

        Analyzes:
        - Campaign efficiency (ROAS, CPA)
        - Budget utilization rates
        - Impression share lost to budget
        - Optimal budget reallocation

        Args:
            customer_id: Google Ads customer ID
            date_range: Date range for analysis
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)

            campaigns = await service.get_campaign_performance(customer_id, date_range=date_range)

            data_summary = f"""CAMPAIGN BUDGET ANALYSIS ({date_range}):

"""
            total_cost = 0
            total_conv = 0
            total_value = 0

            for c in campaigns.get('campaigns', []):
                total_cost += c['cost']
                total_conv += c['conversions']
                total_value += c['conversion_value']

                roas = (c['conversion_value'] / c['cost']) if c['cost'] > 0 else 0
                cpa = (c['cost'] / c['conversions']) if c['conversions'] > 0 else 0

                data_summary += f"""
{c['campaign_name']}:
  - Status: {c['status']}
  - Cost: ${c['cost']:,.2f}
  - Impressions: {c['impressions']:,}
  - Clicks: {c['clicks']:,}
  - CTR: {c['ctr']:.2f}%
  - Conversions: {c['conversions']:.1f}
  - Conv Value: ${c['conversion_value']:,.2f}
  - CPA: ${cpa:.2f}
  - ROAS: {roas:.2f}x
"""

            overall_roas = (total_value / total_cost) if total_cost > 0 else 0
            overall_cpa = (total_cost / total_conv) if total_conv > 0 else 0

            data_summary += f"""
ACCOUNT TOTALS:
- Total Spend: ${total_cost:,.2f}
- Total Conversions: {total_conv:.1f}
- Total Conv Value: ${total_value:,.2f}
- Overall CPA: ${overall_cpa:.2f}
- Overall ROAS: {overall_roas:.2f}x
"""

            # Get LLM analysis
            from services.llm.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw data:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": """You are a budget optimization expert for Google Ads. Analyze this data and provide:

1. **Efficiency Ranking** (rank campaigns by ROI/ROAS)

2. **Budget Recommendations**:
   - Campaigns to increase budget (high ROAS, limited by budget)
   - Campaigns to decrease budget (low ROAS, inefficient)
   - Campaigns to pause (consistently unprofitable)

3. **Reallocation Plan**:
   - Specific $ amounts to move between campaigns
   - Expected impact on conversions and ROAS

4. **Quick Wins** (immediate budget changes for better ROI)

5. **Risk Assessment** (potential downsides of changes)

Be specific with campaign names, dollar amounts, and expected outcomes."""
                },
                {
                    "role": "user",
                    "content": f"Optimize budget allocation:\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=2000
            )

            return f"Budget Optimization Recommendations ({date_range})\n{'='*50}\n\n{response['content']}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error in budget optimization: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def ga_generate_report(
        auth_context: Optional[Dict] = None,
        customer_id: str = None,
        date_range: str = "LAST_30_DAYS",
        report_type: str = "executive"
    ) -> str:
        """
        Generate an AI-powered executive summary report.

        Report types:
        - executive: High-level summary for stakeholders
        - detailed: In-depth performance analysis
        - optimization: Focus on improvement opportunities

        Args:
            customer_id: Google Ads customer ID
            date_range: Date range for report
            report_type: Type of report (executive, detailed, optimization)
        """
        try:
            if not customer_id:
                return "customer_id is required"

            config = await get_google_ads_config()
            if not config:
                return "Google Ads not configured"

            service = GoogleAdsService(config)

            # Fetch comprehensive data
            import asyncio
            account, campaigns, keywords, search_terms = await asyncio.gather(
                service.get_account_performance(customer_id, date_range=date_range),
                service.get_campaign_performance(customer_id, date_range=date_range),
                service.get_keyword_performance(customer_id, date_range=date_range),
                service.get_search_terms(customer_id, date_range=date_range, limit=100)
            )

            # Format comprehensive data
            acc = account.get('metrics', {})
            data_summary = f"""GOOGLE ADS PERFORMANCE REPORT
Period: {date_range}
Account: {account.get('account', {}).get('name', 'N/A')}

ACCOUNT SUMMARY:
- Total Impressions: {acc.get('impressions', 0):,}
- Total Clicks: {acc.get('clicks', 0):,}
- CTR: {acc.get('ctr', 0):.2f}%
- Total Spend: ${acc.get('cost', 0):,.2f}
- Total Conversions: {acc.get('conversions', 0):.1f}
- Conversion Value: ${acc.get('conversion_value', 0):,.2f}

CAMPAIGN BREAKDOWN:
"""
            for c in campaigns.get('campaigns', [])[:10]:
                roas = (c['conversion_value'] / c['cost']) if c['cost'] > 0 else 0
                data_summary += f"- {c['campaign_name']}: ${c['cost']:,.2f} spend, {c['conversions']:.1f} conv, {roas:.2f}x ROAS\n"

            data_summary += f"\n\nTOP KEYWORDS:\n"
            for kw in keywords.get('keywords', [])[:10]:
                data_summary += f"- {kw['keyword']}: {kw['clicks']} clicks, ${kw['cost']:.2f}, {kw['conversions']:.1f} conv\n"

            data_summary += f"\n\nTOP SEARCH TERMS:\n"
            converting_terms = [st for st in search_terms.get('search_terms', []) if st['conversions'] > 0]
            for st in converting_terms[:10]:
                data_summary += f"- \"{st['search_term']}\": {st['conversions']:.1f} conv, ${st['cost']:.2f}\n"

            # Customize prompt based on report type
            report_prompts = {
                "executive": """Create a concise executive summary suitable for stakeholders. Include:
1. Performance Highlights (3-4 key metrics)
2. Wins This Period
3. Areas of Concern
4. Key Recommendations (top 3)
Keep it under 500 words, focus on business impact.""",

                "detailed": """Create a detailed performance report. Include:
1. Executive Summary
2. Campaign-by-Campaign Analysis
3. Keyword Performance Insights
4. Search Term Analysis
5. Trend Analysis
6. Detailed Recommendations
Be thorough but organized.""",

                "optimization": """Create an optimization-focused report. Include:
1. Quick Wins (immediate optimizations)
2. Medium-term Improvements
3. Strategic Changes
4. Budget Reallocation Recommendations
5. Testing Suggestions
Focus on actionable items with expected impact."""
            }

            prompt_instruction = report_prompts.get(report_type, report_prompts["executive"])

            # Get LLM analysis
            from services.llm.llm_manager import get_llm_manager
            llm_manager = get_llm_manager()
            if not llm_manager:
                return f"LLM unavailable. Raw data:\n\n{data_summary}"

            try:
                chat_provider = llm_manager.get_chat_provider()
            except ValueError:
                return f"No chat provider configured.\n\n{data_summary}"

            messages = [
                {
                    "role": "system",
                    "content": f"You are a Google Ads reporting specialist. {prompt_instruction}"
                },
                {
                    "role": "user",
                    "content": f"Generate a {report_type} report from this data:\n\n{data_summary}"
                }
            ]

            response = await chat_provider.chat_completion(
                messages=messages,
                temperature=0.3,
                max_tokens=3000
            )

            return f"Google Ads {report_type.title()} Report ({date_range})\n{'='*50}\n\n{response['content']}"

        except GoogleAdsAPIError as e:
            logger.error(f"Google Ads API error: {e}")
            return f"Google Ads API error: {e}"
        except Exception as e:
            logger.error(f"Error generating report: {e}", exc_info=True)
            return "Operation failed. Check server logs for details."

    # =========================================================================
    # TOKEN MANAGEMENT TOOLS
    # =========================================================================

    async def ga_start_token_refresh(auth_context: Optional[Dict] = None) -> str:
        """
        Start the Google Ads OAuth token refresh flow.

        Returns an authorization URL that the user should open in their browser.
        After authorizing, Google redirects back to the server which automatically
        captures the new refresh token, updates .env, and reloads settings.

        No parameters needed - uses existing GOOGLE_ADS_CLIENT_ID from settings.
        """
        import urllib.parse

        settings = get_settings()
        client_id = settings.google_ads_client_id
        if not client_id:
            return "Google Ads not configured. Set GOOGLE_ADS_CLIENT_ID in .env"

        # Determine redirect URI based on environment
        redirect_uri = "https://mcp.llm-exchange.com/v3/oauth/google-ads/callback"

        params = urllib.parse.urlencode({
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": "https://www.googleapis.com/auth/adwords",
            "response_type": "code",
            "access_type": "offline",
            "prompt": "consent",
        })
        auth_url = f"https://accounts.google.com/o/oauth2/auth?{params}"

        return (
            f"Open this URL in your browser to refresh the Google Ads token:\n\n"
            f"{auth_url}\n\n"
            f"After authorizing, Google will redirect to the server which will "
            f"automatically save the new refresh token.\n\n"
            f"Redirect URI (must be registered in Google Cloud Console):\n"
            f"{redirect_uri}"
        )

    # =========================================================================
    # INPUT SCHEMAS
    # =========================================================================

    _no_params = {"type": "object", "properties": {}, "required": []}
    _customer_id_only = {
        "type": "object",
        "properties": {"customer_id": {"type": "string", "description": "Google Ads customer ID"}},
        "required": ["customer_id"]
    }
    _customer_date = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "date_range": {"type": "string", "description": "Date range: LAST_7_DAYS, LAST_30_DAYS, LAST_90_DAYS, etc.", "default": "LAST_30_DAYS"}
        },
        "required": ["customer_id"]
    }
    _customer_campaign_date = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "campaign_id": {"type": "string", "description": "Campaign ID (optional)"},
            "date_range": {"type": "string", "description": "Date range", "default": "LAST_30_DAYS"}
        },
        "required": ["customer_id"]
    }
    _customer_campaign = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "campaign_id": {"type": "string", "description": "Campaign ID"}
        },
        "required": ["customer_id", "campaign_id"]
    }

    # Utility
    ga_test_connection._input_schema = _no_params
    ga_list_accessible_customers._input_schema = _no_params
    ga_get_account_info._input_schema = _customer_id_only

    # Campaign Management
    ga_list_campaigns._input_schema = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "include_metrics": {"type": "boolean", "description": "Include performance metrics", "default": False},
            "status_filter": {"type": "string", "description": "Filter: ENABLED, PAUSED, REMOVED"}
        },
        "required": ["customer_id"]
    }
    ga_get_campaign._input_schema = _customer_campaign
    ga_create_campaign._input_schema = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "name": {"type": "string", "description": "Campaign name"},
            "budget_amount": {"type": "number", "description": "Daily budget amount"},
            "advertising_channel_type": {"type": "string", "description": "SEARCH, DISPLAY, SHOPPING, VIDEO", "default": "SEARCH"},
            "status": {"type": "string", "description": "ENABLED or PAUSED", "default": "PAUSED"}
        },
        "required": ["customer_id", "name", "budget_amount"]
    }
    ga_update_campaign._input_schema = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "campaign_id": {"type": "string", "description": "Campaign to update"},
            "name": {"type": "string", "description": "New campaign name"},
            "status": {"type": "string", "description": "ENABLED or PAUSED"},
            "budget_amount": {"type": "number", "description": "New daily budget"}
        },
        "required": ["customer_id", "campaign_id"]
    }
    ga_set_campaign_status._input_schema = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "campaign_id": {"type": "string", "description": "Campaign ID"},
            "status": {"type": "string", "description": "ENABLED, PAUSED, or REMOVED"}
        },
        "required": ["customer_id", "campaign_id", "status"]
    }

    # Ad Group Management
    ga_list_ad_groups._input_schema = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "campaign_id": {"type": "string", "description": "Filter by campaign (optional)"}
        },
        "required": ["customer_id"]
    }
    ga_create_ad_group._input_schema = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "campaign_id": {"type": "string", "description": "Campaign ID"},
            "name": {"type": "string", "description": "Ad group name"},
            "cpc_bid": {"type": "number", "description": "CPC bid amount", "default": 1.0},
            "status": {"type": "string", "description": "ENABLED or PAUSED", "default": "ENABLED"}
        },
        "required": ["customer_id", "campaign_id", "name"]
    }
    ga_update_ad_group._input_schema = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "ad_group_id": {"type": "string", "description": "Ad group to update"},
            "name": {"type": "string", "description": "New name"},
            "status": {"type": "string", "description": "ENABLED or PAUSED"},
            "cpc_bid": {"type": "number", "description": "New CPC bid"}
        },
        "required": ["customer_id", "ad_group_id"]
    }

    # Reporting
    ga_query._input_schema = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "query": {"type": "string", "description": "GAQL query string"}
        },
        "required": ["customer_id", "query"]
    }
    ga_get_campaign_performance._input_schema = _customer_campaign_date
    ga_get_account_performance._input_schema = _customer_date
    ga_get_keyword_performance._input_schema = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "campaign_id": {"type": "string", "description": "Campaign ID (optional)"},
            "ad_group_id": {"type": "string", "description": "Ad group ID (optional)"},
            "date_range": {"type": "string", "description": "Date range", "default": "LAST_30_DAYS"}
        },
        "required": ["customer_id"]
    }

    # AI Analysis
    ga_audit_account._input_schema = _customer_date
    ga_analyze_keywords._input_schema = _customer_campaign_date
    ga_analyze_search_terms._input_schema = _customer_campaign_date
    ga_suggest_negative_keywords._input_schema = _customer_campaign_date
    ga_analyze_competitors._input_schema = _customer_campaign_date
    ga_analyze_quality_scores._input_schema = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "campaign_id": {"type": "string", "description": "Campaign ID (optional)"}
        },
        "required": ["customer_id"]
    }
    ga_analyze_trends._input_schema = _customer_campaign_date
    ga_analyze_audiences._input_schema = _customer_campaign_date
    ga_optimize_budget._input_schema = _customer_date
    ga_generate_report._input_schema = {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string", "description": "Google Ads customer ID"},
            "date_range": {"type": "string", "description": "Date range", "default": "LAST_30_DAYS"},
            "report_type": {"type": "string", "description": "Report type: executive, detailed, optimization", "default": "executive"}
        },
        "required": ["customer_id"]
    }

    # Token Management
    ga_start_token_refresh._input_schema = _no_params

    # =========================================================================
    # REGISTER ALL TOOLS
    # =========================================================================

    tools = {
        # Utility
        "GA_test_connection": ga_test_connection,
        "GA_list_accessible_customers": ga_list_accessible_customers,
        "GA_get_account_info": ga_get_account_info,
        # Campaign Management
        "GA_list_campaigns": ga_list_campaigns,
        "GA_get_campaign": ga_get_campaign,
        "GA_create_campaign": ga_create_campaign,
        "GA_update_campaign": ga_update_campaign,
        "GA_set_campaign_status": ga_set_campaign_status,
        # Ad Group Management
        "GA_list_ad_groups": ga_list_ad_groups,
        "GA_create_ad_group": ga_create_ad_group,
        "GA_update_ad_group": ga_update_ad_group,
        # Reporting
        "GA_query": ga_query,
        "GA_get_campaign_performance": ga_get_campaign_performance,
        "GA_get_account_performance": ga_get_account_performance,
        "GA_get_keyword_performance": ga_get_keyword_performance,
        # AI Analysis
        "GA_audit_account": ga_audit_account,
        "GA_analyze_keywords": ga_analyze_keywords,
        "GA_analyze_search_terms": ga_analyze_search_terms,
        "GA_suggest_negative_keywords": ga_suggest_negative_keywords,
        "GA_analyze_competitors": ga_analyze_competitors,
        "GA_analyze_quality_scores": ga_analyze_quality_scores,
        "GA_analyze_trends": ga_analyze_trends,
        "GA_analyze_audiences": ga_analyze_audiences,
        "GA_optimize_budget": ga_optimize_budget,
        "GA_generate_report": ga_generate_report,
        # Token Management
        "GA_start_token_refresh": ga_start_token_refresh,
    }

    for name, handler in tools.items():
        server._tool_handlers[name] = handler

    logger.info(f"Registered {len(tools)} Google Ads tools")
