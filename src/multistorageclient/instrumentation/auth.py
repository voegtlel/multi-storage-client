# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
from typing import Dict, Any, Optional

logger = logging.Logger(__name__)


class AccessTokenProvider:
    def __init__(self, auth_options: Dict[str, Any]):
        self.auth_options = auth_options

    def _require_refresh(self) -> bool:
        return False

    def _refresh_token(self) -> Any:
        return None

    def get_token(self) -> Any:
        return None


class AzureAccessTokenProvider(AccessTokenProvider):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.azure_scopes = self.auth_options.pop("scopes")
        except KeyError as e:
            logger.error("Error: 'scopes' key is missing in auth options")
            raise e

        import msal

        self.msal_client = msal.ConfidentialClientApplication(**self.auth_options)

    def get_token(self):
        try:
            # since msal 1.23, acquire_token_for_client stores tokens in cache and handles expired token automatically
            result = self.msal_client.acquire_token_for_client(scopes=self.azure_scopes)
            if result:
                if "access_token" in result:
                    return result["access_token"]
                else:
                    logger.warning(
                        f"no access token available in response: {result.get('error')}, description: {result.get('error_description')}"
                    )
            else:
                logger.warning("authn response from msal client is empty")
            return None
        except Exception as e:
            logger.error(f"Exception during token fetching: {e}")
            return None


class AccessTokenProviderFactory:
    @staticmethod
    def create_access_token_provider(auth_config: Dict[str, Any]) -> Optional[AccessTokenProvider]:
        if not auth_config:
            return None
        auth_type = auth_config.get("type", None)
        auth_options = auth_config.get("options", {})

        if auth_type == "azure":
            return AzureAccessTokenProvider(auth_options)
        else:
            raise ValueError(f"auth_type: {auth_type} is not supported")
