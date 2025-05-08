import requests
import json
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class DataMeshManager:
    """
    Client for the Data Mesh Manager API.
    Implements endpoints for Data Products and Data Contracts.
    """

    def __init__(self, base_url: str = "https://api.datamesh-manager.com", api_key: Optional[str] = None):
        """
        Initialize the Data Mesh Manager client.

        Args:
            base_url: Base URL for the Data Mesh Manager API
            api_key: Optional API key for authentication
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()

        # Set default headers
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

        # Add API key to headers if provided
        if api_key:
            self.session.headers.update({"x-api-key": api_key})

    def _handle_response(self, response: requests.Response) -> Dict[str, Any]:
        """Handle API response and potential errors."""
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP Error: {e}"
            try:
                error_json = response.json()
                error_msg += f" - {error_json}"
            except:
                error_msg += f" - {response.text}"
            logger.error(error_msg)
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Exception: {e}")
            raise
        except json.JSONDecodeError:
            logger.error(f"JSON Decode Error: Unable to parse response as JSON: {response.text}")
            raise

    # Data Products Endpoints

    def list_data_products(self,
                          page: int = 1,
                          page_size: int = 100,
                          filter_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        List data products with optional filtering.

        Args:
            page: Page number for pagination
            page_size: Number of items per page
            filter_params: Optional filter parameters

        Returns:
            JSON response with data products
        """
        url = f"{self.base_url}/api/dataproducts"
        params = {
            "page": page,
            "pageSize": page_size,
        }

        if filter_params:
            params.update(filter_params)

        response = self.session.get(url, params=params)
        return self._handle_response(response)

    def get_data_product(self, data_product_id: str) -> Dict[str, Any]:
        """
        Get details for a specific data product.

        Args:
            data_product_id: ID of the data product

        Returns:
            Data product details
        """
        url = f"{self.base_url}/api/dataproducts/{data_product_id}"
        response = self.session.get(url)
        return self._handle_response(response)

    # Data Contracts Endpoints

    def list_data_contracts(self,
                           page: int = 1,
                           page_size: int = 100,
                           filter_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        List data contracts with optional filtering.

        Args:
            page: Page number for pagination
            page_size: Number of items per page
            filter_params: Optional filter parameters

        Returns:
            JSON response with data contracts
        """
        url = f"{self.base_url}/api/datacontracts"
        params = {
            "page": page,
            "pageSize": page_size,
        }

        if filter_params:
            params.update(filter_params)

        response = self.session.get(url, params=params)
        return self._handle_response(response)

    def get_data_contract(self, data_contract_id: str) -> Dict[str, Any]:
        """
        Get details for a specific data contract.

        Args:
            data_contract_id: ID of the data contract

        Returns:
            Data contract details
        """
        url = f"{self.base_url}/api/datacontracts/{data_contract_id}"
        response = self.session.get(url)
        return self._handle_response(response)