"""Local Conjur interface backed exclusively by environment variables."""
import os

class Client:
    def __init__(self, connection_info, **kwargs):
        if connection_info.conjur_url != 'local://environment':
            raise ValueError('Local adapter only supports local://environment')

    def get(self, key):
        name = key.removeprefix('env:')
        try:
            return os.environ[name].encode('utf-8')
        except KeyError:
            raise ValueError(f'Local secret environment variable {name!r} is not set') from None
