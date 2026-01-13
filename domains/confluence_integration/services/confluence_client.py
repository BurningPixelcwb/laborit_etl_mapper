"""
Confluence REST API Client
"""

import requests
from typing import Dict, Any, Optional
from requests.auth import HTTPBasicAuth


class ConfluenceClient:
    """Client for interacting with Confluence REST API"""
    
    def __init__(self, url: str, username: str, api_token: str, space_key: str):
        """
        Initialize Confluence client
        
        Args:
            url: Confluence base URL (e.g., https://laborit.atlassian.net/wiki)
            username: Confluence username/email
            api_token: Confluence API token
            space_key: Confluence space key
        """
        self.base_url = url.rstrip('/')
        self.username = username
        self.api_token = api_token
        self.space_key = space_key
        self.auth = HTTPBasicAuth(username, api_token)
        self.api_url = f"{self.base_url}/rest/api"
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Make HTTP request to Confluence API
        
        Args:
            method: HTTP method (GET, POST, PUT, etc.)
            endpoint: API endpoint (relative to /rest/api)
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))
        
        response = requests.request(
            method=method,
            url=url,
            auth=self.auth,
            headers=headers,
            **kwargs
        )
        
        # Raise exception for HTTP errors
        response.raise_for_status()
        return response
    
    def get_page_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """
        Get page by title in the configured space
        
        Args:
            title: Page title
            
        Returns:
            Page data dictionary or None if not found
        """
        try:
            # Search for page by title in space
            endpoint = "content"
            params = {
                'title': title,
                'spaceKey': self.space_key,
                'expand': 'version,ancestors'
            }
            
            response = self._make_request('GET', endpoint, params=params)
            results = response.json()
            
            # Find exact title match (API may return partial matches)
            for page in results.get('results', []):
                if page.get('title') == title:
                    return page
            
            return None
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception:
            return None
    
    def get_page_by_title_and_parent(self, title: str, parent_id: str) -> Optional[Dict[str, Any]]:
        """
        Get page by title that is a child of the specified parent page
        
        Args:
            title: Page title
            parent_id: Parent page ID
            
        Returns:
            Page data dictionary or None if not found
        """
        try:
            # Search for page by title in space
            endpoint = "content"
            params = {
                'title': title,
                'spaceKey': self.space_key,
                'expand': 'version,ancestors'
            }
            
            response = self._make_request('GET', endpoint, params=params)
            results = response.json()
            
            # Find exact title match that is a child of the specified parent
            for page in results.get('results', []):
                if page.get('title') == title:
                    # Check if this page is a child of the specified parent
                    ancestors = page.get('ancestors', [])
                    # The immediate parent is the last ancestor in the list
                    if ancestors:
                        immediate_parent_id = ancestors[-1].get('id')
                        if immediate_parent_id == parent_id:
                            return page
            
            return None
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception:
            return None
    
    def create_page(
        self,
        title: str,
        content: str,
        parent_id: Optional[str] = None,
        content_format: str = 'wiki'
    ) -> Dict[str, Any]:
        """
        Create a new page in Confluence
        
        Args:
            title: Page title
            content: Page content (Wiki Markup or Storage format)
            parent_id: Optional parent page ID (for creating child pages)
            content_format: Content format ('wiki' or 'storage')
            
        Returns:
            Created page data dictionary
        """
        # Build ancestors if parent_id is provided
        ancestors = []
        if parent_id:
            ancestors = [{'id': parent_id}]
        
        # Map content format
        format_map = {
            'wiki': 'wiki',
            'storage': 'storage'
        }
        confluence_format = format_map.get(content_format, 'wiki')
        
        payload = {
            'type': 'page',
            'title': title,
            'space': {'key': self.space_key},
            'body': {
                confluence_format: {
                    'value': content,
                    'representation': confluence_format
                }
            }
        }
        
        if ancestors:
            payload['ancestors'] = ancestors
        
        response = self._make_request('POST', 'content', json=payload)
        return response.json()
    
    def update_page(
        self,
        page_id: str,
        title: str,
        content: str,
        version: int,
        parent_id: Optional[str] = None,
        content_format: str = 'wiki'
    ) -> Dict[str, Any]:
        """
        Update an existing page in Confluence
        
        Args:
            page_id: Page ID to update
            title: New page title
            content: New page content
            version: Current page version (must be incremented)
            parent_id: Optional parent page ID (for moving page)
            content_format: Content format ('wiki' or 'storage')
            
        Returns:
            Updated page data dictionary
        """
        # Get current page to preserve structure
        try:
            current_page = self._make_request('GET', f'content/{page_id}?expand=version,ancestors').json()
        except Exception:
            current_page = {}
        
        # Build ancestors - only include if explicitly changing parent
        # If parent_id is None, we don't include ancestors in payload to preserve existing parent
        ancestors = None
        if parent_id is not None:
            # Explicitly changing parent
            ancestors = [{'id': parent_id}]
        # If parent_id is None, don't include ancestors in payload - Confluence will preserve existing
        
        # Map content format
        format_map = {
            'wiki': 'wiki',
            'storage': 'storage'
        }
        confluence_format = format_map.get(content_format, 'wiki')
        
        payload = {
            'id': page_id,
            'type': 'page',
            'title': title,
            'version': {'number': version},
            'body': {
                confluence_format: {
                    'value': content,
                    'representation': confluence_format
                }
            }
        }
        
        # Only include ancestors if we're explicitly changing the parent
        if ancestors is not None:
            payload['ancestors'] = ancestors
        
        response = self._make_request('PUT', f'content/{page_id}', json=payload)
        return response.json()
    
    def get_page(self, page_id: str, expand: Optional[str] = None) -> Dict[str, Any]:
        """
        Get a page by ID
        
        Args:
            page_id: Page ID
            expand: Optional expand parameter (e.g., 'ancestors,version')
            
        Returns:
            Page data dictionary
        """
        endpoint = f'content/{page_id}'
        if expand:
            endpoint += f'?expand={expand}'
        response = self._make_request('GET', endpoint)
        return response.json()
    
    def verify_parent_page(self, parent_id: str) -> bool:
        """
        Verify that a parent page exists
        
        Args:
            parent_id: Parent page ID
            
        Returns:
            True if parent exists, False otherwise
        """
        try:
            page = self.get_page(parent_id)
            return bool(page.get('id'))
        except Exception:
            return False

