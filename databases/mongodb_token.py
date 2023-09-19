import time

from constants.mongodb_token_constants import MongoDBTokenConstant
from constants.time_constants import TimeConstants
from utils.list_dict_utils import sort_log, get_logs_in_time
from utils.logger_utils import get_logger
from config import TokenMongoDBConfig

from pymongo import MongoClient

logger = get_logger('MongoDB Token')


class MongoDBToken:
    def __init__(self, graph=None):
        if graph is None:
            graph = TokenMongoDBConfig.CONNECTION_URL

        self.mongo = MongoClient(graph)
        self._db = self.mongo[TokenMongoDBConfig.TOKEN_DATABASE]

        self._tokens_col = self._db[MongoDBTokenConstant.TOKENS_COL]
        self._merged_tokens_col = self._db[MongoDBTokenConstant.MERGED_TOKENS_COL]
        self._token_price_col = self._db[MongoDBTokenConstant.TOKEN_PRICE_COL]
        self._merged_token_price_col = self._db[MongoDBTokenConstant.MERGED_TOKEN_PRICE_COL]

        self._merged_token_credit_score_x1 = self._db['merged_token_credit_score_x1']
        self._merged_token_credit_score_x2 = self._db['merged_token_credit_score_x2']
        self._merged_token_credit_score_x3 = self._db['merged_token_credit_score_x3']
        self._merged_token_credit_score_x4 = self._db['merged_token_credit_score_x4']
        self._merged_token_credit_score_x5 = self._db['merged_token_credit_score_x5']
        self._merged_token_credit_score_x6 = self._db['merged_token_credit_score_x6']
        self._merged_token_credit_score_x7 = self._db['merged_token_credit_score_x6']

    def get_7_differences(self, coin_id, start_timestamp, end_timestamp):
        seven_diffs = list()
        for x in range(1, 8):
            pipeline = [
                {
                    '$match': {
                        '_id': coin_id
                    }
                }, {
                '$project': {
                    'maxSubtraction': {
                        '$subtract': [
                            f'$creditScorex{x}ChangeLogs.{end_timestamp}', f'$creditScorex{x}ChangeLogs.{start_timestamp}'
                        ]
                    }
                }
            }
            ]
            result = list(self._db[f'merged_token_credit_score_x{x}'].aggregate(pipeline))
            seven_diffs.append(result[0]['maxSubtraction'])

        return seven_diffs