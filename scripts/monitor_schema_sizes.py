import os
import sys
sys.path.append(os.path.dirname(sys.path[0]))
import time
import pandas as pd
from cli_scheduler.scheduler_job import SchedulerJob

from constants.time_constants import TimeConstants
from databases.postgresql import PostgresDB
from utils.time_utils import human_readable_date, round_timestamp

DIR_PATH = os.environ.get('DIR_PATH')
N_DAYS = int(os.environ.get('N_DAYS'))
#DIR_PATH = '../data'
#DIR_PATH = '/home/xuantung/Tovchain/'


def monitor_token_by_schema():
    postgres = PostgresDB()
    today = round_timestamp(int(time.time()))
    time_deltas = list(range(0, N_DAYS + 1, 1))

    data = postgres.get_schema_size()
    table_sizes_dict = {
        'time': [human_readable_date(today)],
    }

    for chain, chain_data in data.items():
        chain_data['time'] = human_readable_date(today)
        df = pd.DataFrame([chain_data])
        df.set_index(keys=['time'], inplace=True)

        file_name = f'{DIR_PATH}/{chain}_data.csv'
        try:
            existing_df = pd.read_csv(file_name, index_col='time')
            existing_df = pd.concat([existing_df, df])
        except FileNotFoundError:
            existing_df = df
        existing_df.to_csv(file_name)


class MonitorTablesJob(SchedulerJob):
    def __init__(self, run_now, interval, delay):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)

    def _pre_start(self):
        self.postgres = PostgresDB()
        self.schemas = ['chain_0x38']

    def _execute(self):
        for schema in self.schemas:
            monitor_token_by_schema()


if __name__ == '__main__':
    job = MonitorTablesJob(run_now=True,
                           interval=TimeConstants.A_DAY,
                           delay=TimeConstants.A_HOUR*2)
    job.run()