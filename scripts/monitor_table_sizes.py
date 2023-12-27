import os
import sys
sys.path.append(os.path.dirname(sys.path[0]))
import time
import pandas as pd
from cli_scheduler.scheduler_job import SchedulerJob

from constants.time_constants import TimeConstants
from databases.postgresql import PostgresDB
from utils.time_utils import human_readable_date, round_timestamp, human_readable_time

DIR_PATH = os.environ.get('DIR_PATH')
# N_DAYS = 93


def monitor_table_sizes(table_name: str):
    postgres = PostgresDB()
    today = round_timestamp(int(time.time()))

    table_sizes_data = postgres.get_tables_sizes_on_all_schema(table_name=table_name)
    table_sizes_dict = {
        'time': [human_readable_date(today)],
    }

    for _table_name, _table_size in table_sizes_data.items():
        table_sizes_dict[_table_name] = [_table_size]
    latest_tablesize_df = pd.DataFrame.from_dict(table_sizes_dict)
    latest_tablesize_df.set_index(keys=['time'], inplace=True)

    file_path = f'{DIR_PATH}/{table_name}.csv'
    try:
        tablesize_df = pd.read_csv(file_path, index_col='time')
        tablesize_df = pd.concat([tablesize_df, latest_tablesize_df])
    except FileNotFoundError:
        tablesize_df = latest_tablesize_df
    tablesize_df.to_csv(file_path)


class MonitorTablesJob(SchedulerJob):
    def __init__(self, tables: list[str],
                 run_now, interval, delay):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        self.tables = tables
        super().__init__(scheduler)

    def _pre_start(self):
        self.postgres = PostgresDB()

    def _execute(self):
        for _table_name in self.tables:
            monitor_table_sizes(_table_name)


if __name__ == '__main__':
    tables = ['amount_in_out', 'dapp_interaction', 'smart_contract', 'token_decimals', 'token_transfer', 'wrapped_token']
    job = MonitorTablesJob(tables=tables,
                           run_now=True,
                           interval=TimeConstants.A_DAY,
                           delay=TimeConstants.A_HOUR*2)
    job.run()