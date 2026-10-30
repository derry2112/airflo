"""Retrieve and mask secrets using the Conjur token authentication client."""

import logging
import os
import time
from urllib.parse import quote

from airflow.sdk.log import mask_secret
from conjur_api import Client
from conjur_api.models import ConjurConnectionInfo, CredentialsData, SslVerificationMode
from conjur_api.providers import SimpleCredentialsProvider, TokenAuthenticationStrategy


class ConjurSecretManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.init_conjur()

    def init_conjur(self):
        self.conjur_url = os.environ["CONJUR_APPLIANCE_URL"]
        self.account = os.environ["CONJUR_ACCOUNT"]
        self.role_id = os.environ["CONJUR_AUTHN_LOGIN"]
        self.ssl_verification_mode = SslVerificationMode.INSECURE
        self.connection_info = ConjurConnectionInfo(
            conjur_url=self.conjur_url, account=self.account, proxy_params=None
        )
        self.token_file = os.environ["CONJUR_AUTHN_TOKEN_FILE"]
        self.client = self.__fetch_conjur_client()

    def __fetch_conjur_client(self) -> Client:
        with open(self.token_file, "r") as token_file:
            token = token_file.read()
        credentials = CredentialsData(
            username=self.role_id, api_token=token, machine=self.conjur_url
        )
        credentials_provider = SimpleCredentialsProvider()
        credentials_provider.save(credentials)
        authn_provider = TokenAuthenticationStrategy(credentials_provider)
        return Client(
            self.connection_info,
            authn_strategy=authn_provider,
            ssl_verification_mode=self.ssl_verification_mode,
            async_mode=False,
        )

    def get_conjur_value(self, key: str) -> str:
        """Return a URL-encoded secret for a database connection string."""
        return self._get_conjur_value(key, encode=True)

    def get_conjur_value_unparsed(self, key: str) -> str:
        """Return a plain secret for FTP and SFTP credentials."""
        return self._get_conjur_value(key, encode=False)

    def _get_conjur_value(self, key: str, *, encode: bool) -> str:
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            try:
                if attempt > 1:
                    self.init_conjur()
                self.logger.info("Getting value from DAP %s", key)
                secret = self.client.get(key).decode("utf-8").strip()
                mask_secret(secret)
                if encode:
                    secret = quote(secret, safe="")
                    mask_secret(secret)
                return secret
            except Exception:
                self.logger.error("Error getting value from DAP %s", key)
                if attempt == max_attempts:
                    self.logger.error(
                        "Failed to get value from DAP %s after %s attempts.",
                        key, max_attempts,
                    )
                    raise
                self.logger.warning(
                    "Retrying %s... Re-initializing Conjur client.", key
                )
                time.sleep(2)
