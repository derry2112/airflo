"""Local environment-backed replacement for the Conjur adapter.

Airflow connection extras reference environment variable names, for example:
{"username_dap": "LOCAL_SFTP_USERNAME", "password_dap": "LOCAL_SFTP_PASSWORD"}.
Set those variables in the environment of the process running the task.
The optional ``env:`` prefix is also accepted. Values are returned unchanged.
No connection to Conjur is made.
"""

import os


class ConjurSecretManager:
    """Keep the original hook interface while resolving secrets locally."""

    def get_conjur_value_unparsed(self, reference: str | None) -> str | None:
        # An omitted username allows the hook to retain its connection login.
        if reference is None or reference == "":
            return None
        if not isinstance(reference, str):
            raise TypeError("Local secret reference must be an environment variable name")
        name = reference.removeprefix("env:")
        if not name:
            raise ValueError("Local secret environment variable name must not be empty")
        try:
            return os.environ[name]
        except KeyError:
            raise ValueError(
                f"Local secret environment variable {name!r} is not set"
            ) from None

    def get_conjur_value(self, reference: str | None) -> str | None:
        """Resolve FTP secret references using the same local environment store."""
        return self.get_conjur_value_unparsed(reference)
