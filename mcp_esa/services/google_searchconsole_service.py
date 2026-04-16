"""
Google Search Console Service Module
Wraps the Search Console API using OAuth user credentials.
Uses google-api-python-client (discovery API).
"""

import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

try:
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GSC_AVAILABLE = True
except ImportError:
    GSC_AVAILABLE = False
    Credentials = None

logger = logging.getLogger(__name__)

GSC_SCOPES = ["https://www.googleapis.com/auth/webmasters"]
TOKEN_URI = "https://oauth2.googleapis.com/token"


@dataclass
class GSCConfig:
    """GSC OAuth user-credential config."""
    client_id: str
    client_secret: str
    refresh_token: str


class GSCAPIError(Exception):
    """GSC API error."""
    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.details = details or {}


class GSCService:
    """Service for Google Search Console API."""

    def __init__(self, config: GSCConfig):
        if not GSC_AVAILABLE:
            raise ImportError(
                "GSC libraries not installed. Run: "
                "pip install google-api-python-client google-auth"
            )
        self.config = config
        self._creds: Optional[Credentials] = None
        self._search_client = None
        self._webmasters_client = None

    # ------------------------------------------------------------------ creds
    def _get_credentials(self) -> Credentials:
        if self._creds is None:
            if not self.config.refresh_token:
                raise GSCAPIError("Search Console refresh token not configured")
            self._creds = Credentials(
                token=None,
                refresh_token=self.config.refresh_token,
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
                token_uri=TOKEN_URI,
                scopes=GSC_SCOPES,
            )
        return self._creds

    def _searchconsole(self):
        """Get the Search Console API v1 client (searchanalytics, urlInspection)."""
        if self._search_client is None:
            self._search_client = build(
                'searchconsole', 'v1',
                credentials=self._get_credentials(),
                cache_discovery=False,
            )
        return self._search_client

    def _webmasters(self):
        """Get the Webmasters API v3 client (sites, sitemaps)."""
        if self._webmasters_client is None:
            self._webmasters_client = build(
                'webmasters', 'v3',
                credentials=self._get_credentials(),
                cache_discovery=False,
            )
        return self._webmasters_client

    # --------------------------------------------------------------- sites
    async def list_sites(self) -> Dict[str, Any]:
        """List all verified Search Console properties."""
        try:
            result = self._webmasters().sites().list().execute()
            sites = []
            for entry in result.get('siteEntry', []):
                sites.append({
                    'site_url': entry.get('siteUrl', ''),
                    'permission_level': entry.get('permissionLevel', ''),
                })
            return {'status': 'success', 'sites': sites, 'count': len(sites)}
        except HttpError as e:
            raise GSCAPIError(f"list_sites failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"list_sites failed: {e}")

    async def test_connection(self) -> Dict[str, Any]:
        """Validate credentials by listing sites."""
        try:
            result = await self.list_sites()
            return {
                'status': 'success',
                'site_count': result['count'],
                'sites': [s['site_url'] for s in result['sites']],
            }
        except Exception as e:
            logger.error(f"GSC test_connection failed: {e}", exc_info=True)
            return {'status': 'error', 'message': str(e)}

    # --------------------------------------------------------------- search analytics
    async def analyze_keywords(
        self,
        site_url: str,
        start_date: str,
        end_date: str,
        country: Optional[str] = None,
        device: Optional[str] = None,
        page_filter: Optional[str] = None,
        query_filter: Optional[str] = None,
        row_limit: int = 100,
        sort_by: str = 'clicks',
    ) -> Dict[str, Any]:
        """Search performance grouped by query keyword."""
        sort_map = {
            'clicks': 'clicks',
            'impressions': 'impressions',
            'ctr': 'ctr',
            'position': 'position',
        }
        if sort_by not in sort_map:
            raise GSCAPIError(f"Invalid sort_by '{sort_by}'. Must be one of: {list(sort_map.keys())}")

        row_limit = min(max(1, row_limit), 25000)

        body = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['query'],
            'rowLimit': row_limit,
            'startRow': 0,
        }

        # Build dimension filters
        filters = []
        if country:
            filters.append({
                'dimension': 'country',
                'operator': 'equals',
                'expression': country.upper(),
            })
        if device:
            valid_devices = ('DESKTOP', 'MOBILE', 'TABLET')
            device_upper = device.upper()
            if device_upper not in valid_devices:
                raise GSCAPIError(f"Invalid device '{device}'. Must be one of: {valid_devices}")
            filters.append({
                'dimension': 'device',
                'operator': 'equals',
                'expression': device_upper,
            })
        if page_filter:
            filters.append({
                'dimension': 'page',
                'operator': 'contains',
                'expression': page_filter,
            })
        if query_filter:
            filters.append({
                'dimension': 'query',
                'operator': 'contains',
                'expression': query_filter,
            })

        if filters:
            body['dimensionFilterGroups'] = [{
                'groupType': 'and',
                'filters': filters,
            }]

        try:
            response = self._searchconsole().searchanalytics().query(
                siteUrl=site_url, body=body
            ).execute()
        except HttpError as e:
            raise GSCAPIError(f"analyze_keywords failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"analyze_keywords failed: {e}")

        rows = []
        for row in response.get('rows', []):
            keys = row.get('keys', [])
            rows.append({
                'query': keys[0] if keys else '',
                'clicks': row.get('clicks', 0),
                'impressions': row.get('impressions', 0),
                'ctr': round(row.get('ctr', 0), 4),
                'position': round(row.get('position', 0), 1),
            })

        # Sort
        reverse = sort_by != 'position'  # lower position = better
        rows.sort(key=lambda r: r.get(sort_by, 0), reverse=reverse)

        return {
            'status': 'success',
            'site_url': site_url,
            'date_range': {'start_date': start_date, 'end_date': end_date},
            'row_count': len(rows),
            'rows': rows,
        }

    # --------------------------------------------------------------- URL inspection
    async def inspect_url(self, site_url: str, inspection_url: str) -> Dict[str, Any]:
        """Get index status and rich result info for a specific URL."""
        body = {
            'inspectionUrl': inspection_url,
            'siteUrl': site_url,
        }
        try:
            response = self._searchconsole().urlInspection().index().inspect(
                body=body
            ).execute()
        except HttpError as e:
            raise GSCAPIError(f"inspect_url failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"inspect_url failed: {e}")

        result = response.get('inspectionResult', {})
        index_status = result.get('indexStatusResult', {})
        mobile = result.get('mobileUsabilityResult', {})
        rich = result.get('richResultsResult', {})

        return {
            'status': 'success',
            'inspection_url': inspection_url,
            'site_url': site_url,
            'index_status': {
                'verdict': index_status.get('verdict', ''),
                'coverage_state': index_status.get('coverageState', ''),
                'indexing_state': index_status.get('indexingState', ''),
                'last_crawl_time': index_status.get('lastCrawlTime', ''),
                'page_fetch_state': index_status.get('pageFetchState', ''),
                'robots_txt_state': index_status.get('robotsTxtState', ''),
                'crawled_as': index_status.get('crawledAs', ''),
                'referring_urls': index_status.get('referringUrls', []),
            },
            'mobile_usability': {
                'verdict': mobile.get('verdict', ''),
                'issues': mobile.get('issues', []),
            },
            'rich_results': {
                'verdict': rich.get('verdict', ''),
                'detected_items': rich.get('detectedItems', []),
            },
        }

    # --------------------------------------------------------------- sitemaps
    async def list_sitemaps(self, site_url: str) -> Dict[str, Any]:
        """List sitemaps for a property."""
        try:
            response = self._webmasters().sitemaps().list(siteUrl=site_url).execute()
        except HttpError as e:
            raise GSCAPIError(f"list_sitemaps failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"list_sitemaps failed: {e}")

        sitemaps = []
        for s in response.get('sitemap', []):
            sitemaps.append({
                'path': s.get('path', ''),
                'type': s.get('type', ''),
                'is_pending': s.get('isPending', False),
                'last_submitted': s.get('lastSubmitted', ''),
                'last_downloaded': s.get('lastDownloaded', ''),
                'warnings': int(s.get('warnings', 0)),
                'errors': int(s.get('errors', 0)),
            })

        return {'status': 'success', 'site_url': site_url, 'sitemaps': sitemaps, 'count': len(sitemaps)}

    async def submit_sitemap(self, site_url: str, sitemap_url: str) -> Dict[str, Any]:
        """Submit a new sitemap."""
        try:
            self._webmasters().sitemaps().submit(
                siteUrl=site_url, feedpath=sitemap_url
            ).execute()
            return {'status': 'success', 'site_url': site_url, 'sitemap_url': sitemap_url, 'message': 'Sitemap submitted'}
        except HttpError as e:
            raise GSCAPIError(f"submit_sitemap failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"submit_sitemap failed: {e}")

    async def delete_sitemap(self, site_url: str, sitemap_url: str) -> Dict[str, Any]:
        """Delete a sitemap."""
        try:
            self._webmasters().sitemaps().delete(
                siteUrl=site_url, feedpath=sitemap_url
            ).execute()
            return {'status': 'success', 'site_url': site_url, 'sitemap_url': sitemap_url, 'message': 'Sitemap deleted'}
        except HttpError as e:
            raise GSCAPIError(f"delete_sitemap failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"delete_sitemap failed: {e}")

    # --------------------------------------------------------------- coverage
    async def get_coverage(
        self,
        site_url: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Index coverage summary via searchanalytics with page dimension."""
        from datetime import datetime, timedelta

        if not end_date:
            end_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        body = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['page'],
            'rowLimit': 25000,
            'startRow': 0,
        }

        try:
            response = self._searchconsole().searchanalytics().query(
                siteUrl=site_url, body=body
            ).execute()
        except HttpError as e:
            raise GSCAPIError(f"get_coverage failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"get_coverage failed: {e}")

        rows = response.get('rows', [])
        total_clicks = sum(r.get('clicks', 0) for r in rows)
        total_impressions = sum(r.get('impressions', 0) for r in rows)

        return {
            'status': 'success',
            'site_url': site_url,
            'date_range': {'start_date': start_date, 'end_date': end_date},
            'pages_with_data': len(rows),
            'total_clicks': total_clicks,
            'total_impressions': total_impressions,
            'note': 'Pages appearing in search results during the date range. Use GSC_inspect_url for detailed index status of specific URLs.',
        }
