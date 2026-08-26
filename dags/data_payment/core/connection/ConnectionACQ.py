from airflow.providers.postgres.hooks.postgres import PostgresHook


class ACQ_DB(object):
    def __init__(self, connection_id="db_acq_psql"):
        self.hook = PostgresHook(postgres_conn_id=connection_id)

    def get_records(self, sql, params=None):
        return self.hook.get_records(sql=sql, parameters=params)
