import os
import sys
sys.path.append(os.path.dirname(sys.path[0]))
import time
import pandas as pd
from cli_scheduler.scheduler_job import SchedulerJob

from constants.time_constants import TimeConstants, MonitorConstants
from databases.postgresql import PostgresDB
from utils.time_utils import human_readable_date, round_timestamp

DIR_PATH = os.environ.get('DIR_PATH')
N_DAYS = int(os.environ.get('N_DAYS'))


def monitor_detailed_table_size(table_name: str):
    """Get detailed information (total size, index size, number of tuples, etc.) of a table on all schemas
    """
    postgres = PostgresDB()
    today = round_timestamp(int(time.time()))

    data = postgres.get_detailed_table(table_name)

    for chain, chain_data in data.items():
        chain_data['time'] = human_readable_date(today)
        df = pd.DataFrame([chain_data])
        df.set_index(keys=['time'], inplace=True)

        file_name = f'{DIR_PATH}/{chain}_{table_name}.csv'
        try:
            existing_df = pd.read_csv(file_name, index_col='time')
            existing_df = pd.concat([existing_df, df])
        except FileNotFoundError:
            existing_df = df
        existing_df.to_csv(file_name)


class MonitorTablesDetailsJob(SchedulerJob):
    def __init__(self, run_now, interval, delay):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)

    def _pre_start(self):
        self.postgres = PostgresDB()

    def _execute(self):
        table_names = ['token_transfer', 'dapp_interaction']
        for _table_name in table_names:
            monitor_detailed_table_size(_table_name)


if __name__ == '__main__':
    job = MonitorTablesDetailsJob(run_now=True,
                                  interval=TimeConstants.A_DAY,
                                  delay=MonitorConstants.DELAY)
    job.run()
