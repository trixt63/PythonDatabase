import os
import sys
sys.path.append(os.path.dirname(sys.path[0]))
import time
import pandas as pd
from cli_scheduler.scheduler_job import SchedulerJob

from constants.time_constants import TimeConstants
from databases.postgresql import PostgresDB
from utils.time_utils import human_readable_date, round_timestamp

DIR_PATH = 'data'
#DIR_PATH = '../data'
# DIR_PATH = '/home/xuantung/Tovchain/'


def monitor_table_sizes(schema: str):
    postgres = PostgresDB()
    today = round_timestamp(int(time.time()))
    time_deltas = list(range(0, 37, 1))
    FILE_PATH = f'{DIR_PATH}/{schema}_tables_size.csv'
    try:
        schema_df = pd.read_csv(FILE_PATH, index_col=0)
    except FileNotFoundError:
        _columns = ['time', 'amount_in_out', 'dapp_interaction', 'smart_contract', 'token_decimals',
                    'token_transfer', 'wrapped_token']
        _columns.extend([f"balance_change_{time_delta}" for time_delta in time_deltas])
        schema_df = pd.DataFrame(columns=_columns)

    _tables_size_data = postgres.get_tables_size(schema)

    _tables_size_dict = {
        'time': [human_readable_date(today)],
        'amount_in_out': [_tables_size_data.get('amount_in_out', None)],
        'dapp_interaction': [_tables_size_data.get('dapp_interaction', None)],
        'smart_contract': [_tables_size_data.get('smart_contract', None)],
        'token_decimals': [_tables_size_data.get('token_decimals', None)],
        'token_transfer': [_tables_size_data.get('token_transfer', None)],
        'wrapped_token': [_tables_size_data.get('wrapped_token', None)]
    }
    for time_delta in time_deltas:
        _timestamp = today - time_delta * TimeConstants.A_DAY
        _balance_change_table_name = f"balance_change_{_timestamp}"
        _tables_size_dict[f"balance_change_{time_delta}"] = [_tables_size_data.get(_balance_change_table_name, None)]

    schema_df = pd.concat([schema_df, pd.DataFrame(data=_tables_size_dict)])
    schema_df.to_csv(FILE_PATH)


class MonitorTablesJob(SchedulerJob):
    def __init__(self, run_now, interval, delay):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)

    def _pre_start(self):
        self.postgres = PostgresDB()
        self.schemas = ['chain_0x38', 'chain_0x1', 'chain_0xfa', 'chain_0x89',
                        'chain_0xa', 'chain_0xa4b1', 'chain_0xa86a', 'chain_0x2b6653dc']

    def _execute(self):
        for schema in self.schemas:
            monitor_table_sizes(schema)


if __name__ == '__main__':
    job = MonitorTablesJob(run_now=True,
                           interval=TimeConstants.A_DAY,
                           delay=TimeConstants.A_HOUR*2)
    job.run()
