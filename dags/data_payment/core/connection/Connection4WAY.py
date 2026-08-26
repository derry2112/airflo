from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook
import logging


class Way4DB(object):
    def __init__(self):
        self.hook = PostgresHook(postgres_conn_id='db_rekon_uat')

    def get_data(self, sql, params=None):
        try:
            logging.info('Execute SQL')
            logging.info(sql)

            if params:
                logging.info('SQL params: %s', params)
                records = self.hook.get_records(sql=sql, parameters=params)
            else:
                records = self.hook.get_records(sql=sql)
            logging.info('total records:%s', len(records))

            return records
        except Exception as e:
            logging.exception(e)
            raise

    def get_data_way4(self, sql):
        try:
            logging.info('Execute SQL')
            logging.info(sql)

            records = self.hook.get_records(sql=sql)
            logging.info('total records: %s', len(records))

            return records
        except Exception as e:
            logging.exception(e)
            raise


class PwcDB(object):
    def __init__(self):
        self.hook = OracleHook(oracle_conn_id='pwcDB')

    def get_data_pwc(self, sql):
        try:
            logging.info('Execute SQL')
            logging.info(sql)

            records = self.hook.get_records(sql=sql)
            logging.info('total records: %s', len(records))

            return records
        except Exception as e:
            logging.exception(e)
            raise

    def get_fetch_one(self, sql, params):

        try:
            logging.info('=== START GET_FETCH_ONE ===')
            logging.info('Execute SQL')
            logging.info(sql)
            logging.info('SQL params:%s', params)
            logging.info('SQL params type:%s', type(params))

            if params is not None:
                records = self.hook.get_records(sql=sql, parameters=params)
            else:
                records = self.hook.get_records(sql=sql)
            logging.info('Query result:%s', records)
            logging.info('Total records:%s', len(records) if records else 0)
            logging.info('=== END GET_FETCH_ONE ===')
            return records

        except Exception as e:
            logging.exception('Error execute get_fetch_one')
            raise
