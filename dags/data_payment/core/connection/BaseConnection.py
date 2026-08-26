from airflow.providers.postgres.hooks.postgres import PostgresHook


class DBConnection(object):
    def __init__(self, **config):
        self.config = config
        self.hook = PostgresHook(
            postgres_conn_id=self.config.get("connection_id")
        )

    def execute_fetch_many(self, sql, params=None):
        return self.hook.get_records(sql=sql, parameters=params)

    def execute_fetch_one(self, sql, params=None):
        records = self.hook.get_records(sql=sql, parameters=params)
        return records[0] if records else None
