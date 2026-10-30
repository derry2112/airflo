class SimpleCredentialsProvider:
    def save(self, credentials):
        self.credentials = credentials

class TokenAuthenticationStrategy:
    def __init__(self, credentials_provider):
        self.credentials_provider = credentials_provider
