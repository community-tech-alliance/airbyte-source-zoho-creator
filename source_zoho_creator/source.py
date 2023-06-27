#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import enum
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.models import SyncMode

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator


# Basic full refresh stream
class ZohoCreatorStream(HttpStream, ABC):
    """
    Base Full Refresh Stream for Zoho Creator API
    """

    url_base = "https://creator.zoho.com/api/v2/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None
    
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield {}


class Applications(ZohoCreatorStream):
    """
    Stream to sync results for the Applications Endpoint.

    :param authenticator: OAuth Athenticator to generate Access Tokens
    :param username: The username of the of the Application owner
    """

    primary_key = "link_name"
    
    @property
    def use_cache(self) -> bool:
        return True

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "applications"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Parse response for GET Applications endpoint
        :return an iterable containing each record in the response
        """
        yield from response.json().get("applications", [])


class Pages(HttpSubStream, ZohoCreatorStream):
    """
    Stream to sync results for the Applications Endpoint.

    :param authenticator: OAuth Athenticator to generate Access Tokens
    :param username: The 
    """

    primary_key = "link_name"

    def __init__(self, **kwargs):
        super().__init__(Applications(**kwargs), **kwargs)

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{stream_slice['parent']['workspace_name']}/{stream_slice['parent']['link_name']}/pages"

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        """
        Parse response for GET Pages endpoint for each Application
        :return an iterable containing each record in the response
        """
        # Get Parent Stream ID
        parent_application = stream_slice["parent"]["link_name"]
        pages = response.json().get("pages",[])
        for page in pages:
            page["application_link_name"] = parent_application
        yield from pages


class Reports(HttpSubStream, ZohoCreatorStream):
    """
    Stream to sync results for the Reports Endpoint.

    :param authenticator: OAuth Athenticator to generate Access Tokens
    :param username: The 
    """

    primary_key = "link_name"

    def __init__(self, **kwargs):
        super().__init__(Applications(**kwargs), **kwargs)

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{stream_slice['parent']['workspace_name']}/{stream_slice['parent']['link_name']}/reports"

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        """
        Parse response for GET Reports endpoint for each Report
        :return an iterable containing each record in the response
        """
        # Get Parent Stream ID
        parent_application = stream_slice["parent"]["link_name"]
        reports = response.json().get("reports",[])
        for report in reports:
            report["application_link_name"] = parent_application
        yield from reports


class Forms(HttpSubStream, ZohoCreatorStream):
    """
    Stream to sync results for the Forms Endpoint.

    :param authenticator: OAuth Athenticator to generate Access Tokens
    :param username: The 
    """

    primary_key = "link_name"

    @property
    def use_cache(self) -> bool:
        return True

    def __init__(self, **kwargs):
        super().__init__(Applications(**kwargs), **kwargs)

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{stream_slice['parent']['workspace_name']}/{stream_slice['parent']['link_name']}/forms"

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        """
        Parse response for GET Forms endpoint for each Form
        :return an iterable containing each record in the response
        """
        # Get Parent Stream ID
        parent_application = stream_slice["parent"]["link_name"]
        workspace_name = stream_slice["parent"]["workspace_name"]
        forms = response.json().get("forms",[])
        for form in forms:
            form["application_link_name"] = parent_application
            form["workspace_name"] = workspace_name
        yield from forms

  
class Fields(HttpSubStream, Applications):
    """
    Stream to sync results for the Fields Endpoint.

    :param authenticator: OAuth Athenticator to generate Access Tokens
    :param username: The 
    """

    primary_key = "link_name"

    @property
    def use_cache(self) -> bool:
        return True

    def __init__(self, **kwargs):
        super().__init__(Applications(**kwargs),**kwargs)

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Create stream slices for all forms. This function first grabs all cached Applications
        and iterates over them to get all cached Forms. Each of these Forms becomes a stream slice
        for the Fields Stream.

        Note: I wasnt able to substream a substream so this is why this method
        is overwriting the default HttpSubStream stream_slices.
        """
        # Create a Forms stream and its parent Applications stream
        applications_stream = Applications(authenticator=self.authenticator)
        forms_stream = Forms(authenticator=self.authenticator)
        
        # Iterate through Applications stream and create a per application stream slice for the Forms Stream
        for application in applications_stream.read_records(sync_mode=SyncMode.full_refresh):
            # Format stream_slice to match expected HttpSubStream structure for Forms stream
            parent_record = {"parent": application}
            # Iterate through forms and create stream slice for each form
            for form in forms_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=parent_record):
                yield {"parent": form}
        
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return (
            f"{stream_slice['parent']['workspace_name']}/{stream_slice['parent']['application_link_name']}/form/{stream_slice['parent']['link_name']}/fields"
        )
    
    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        """
        Parse response for GET Pages endpoint for each Form
        :return an iterable containing each record in the response
        """
        # Get Parent Stream ID
        parent_application = stream_slice["parent"]["application_link_name"]
        form_link_name = stream_slice["parent"]["link_name"]
        fields = response.json().get("fields",[])
        for field in fields:
            field["application_link_name"] = parent_application
            field["form_link_name"] = form_link_name
        yield from fields

# Source
class SourceZohoCreator(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Gets an OAuth Access Token using the Refresh Token endpoint. This validates the input credentials and that
        the Zoho API is reachable.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        auth = Oauth2Authenticator(
            token_refresh_endpoint="https://accounts.zoho.com/oauth/v2/token",
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            refresh_token=config["refresh_token"]
        )
        try:
            access_token = auth.get_access_token()
            if access_token:
                logger.info(access_token)
                logger.info("Successfully Authenticated to Zoho Creator API.")
                return True, None
        except Exception as error:
            logger.error(f"Unable to retrieve Access token with input credendtials. {error}")
            return False, error
        

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Initiate Streams for all Zoho Creator Endpoints

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = Oauth2Authenticator(
            token_refresh_endpoint="https://accounts.zoho.com/oauth/v2/token",
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            refresh_token=config["refresh_token"]
        )
        streams = [
            Applications(authenticator=auth),
            Pages(authenticator=auth),
            Reports(authenticator=auth),
            Forms(authenticator=auth),
            Fields(authenticator=auth)
        ]
        return streams
