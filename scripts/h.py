import pandas as pd

# Dữ liệu dictionary của bạn
data = {
    'chain_0x89': {'table_name': 'token_transfer', 'total_size': 206916108288, 'data_size': 139304067072, 'index_size': 67573497856, 'n_live_tup': 544155868, 'n_tup_ins': 425763348, 'n_tup_del': 424597282},
    'chain_0x38': {'table_name': 'token_transfer', 'total_size': 201857998848, 'data_size': 134660833280, 'index_size': 67159916544, 'n_live_tup': 516781642, 'n_tup_ins': 438271078, 'n_tup_del': 416888392},
    'chain_0x2b6653dc': {'table_name': 'token_transfer', 'total_size': 89694838784, 'data_size': 47449047040, 'index_size': 42232643584, 'n_live_tup': 175261306, 'n_tup_ins': 150249258, 'n_tup_del': 147039503},
    'chain_0xa4b1': {'table_name': 'token_transfer', 'total_size': 61948641280, 'data_size': 40328134656, 'index_size': 21609332736, 'n_live_tup': 151727462, 'n_tup_ins': 132737683, 'n_tup_del': 88086333},
    'chain_0x1': {'table_name': 'token_transfer', 'total_size': 46160551936, 'data_size': 30534770688, 'index_size': 15617302528, 'n_live_tup': 116599374, 'n_tup_ins': 100055015, 'n_tup_del': 90484232},
    'chain_0xa': {'table_name': 'token_transfer', 'total_size': 47361507328, 'data_size': 31793831936, 'index_size': 15558860800, 'n_live_tup': 114460384, 'n_tup_ins': 101881273, 'n_tup_del': 59573126},
    'chain_0xfa': {'table_name': 'token_transfer', 'total_size': 61252886528, 'data_size': 39508779008, 'index_size': 21733154816, 'n_live_tup': 108444402, 'n_tup_ins': 90230383, 'n_tup_del': 128242125},
    'chain_0xa86a': {'table_name': 'token_transfer', 'total_size': 17975484416, 'data_size': 10436067328, 'index_size': 7536500736, 'n_live_tup': 37900587, 'n_tup_ins': 33021318, 'n_tup_del': 27035235}
}

# Lưu từng file CSV cho mỗi chain
for chain, chain_data in data.items():
    df = pd.DataFrame([chain_data])  # Chuyển đổi từ dictionary thành list vì DataFrame yêu cầu một iterable
    file_name = f'{chain}_data.csv'
    df.to_csv(file_name, index=False)
