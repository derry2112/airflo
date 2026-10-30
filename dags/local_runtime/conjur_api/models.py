from dataclasses import dataclass
from enum import Enum

class SslVerificationMode(Enum):
    INSECURE = 'local-only'

@dataclass
class ConjurConnectionInfo:
    conjur_url: str
    account: str
    proxy_params: object = None

@dataclass
class CredentialsData:
    username: str
    api_token: str
    machine: str
