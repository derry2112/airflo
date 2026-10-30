"""Connection settings shared by database and transfer hooks."""


class ConnectionModel:
    sql: str
    username_dap: str
    password_dap: str
    host: str
    port: str
    schema: str
    target_session_attrs: str
    connect_timeout: int
    service_name: str
    database: str

    def __init__(
        self,
        sql: str = None,
        username_dap: str = None,
        password_dap: str = None,
        host: str = None,
        port: str = None,
        schema: str = None,
        target_session_attrs: str = None,
        connect_timeout: int = None,
        service_name: str = None,
        database: str = None,
    ):
        self.sql = sql
        self.username_dap = username_dap
        self.password_dap = password_dap
        self.host = host
        self.port = port
        self.schema = schema
        self.target_session_attrs = target_session_attrs
        self.connect_timeout = connect_timeout
        self.service_name = service_name
        self.database = database
