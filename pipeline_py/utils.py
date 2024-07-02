import pandas as pd
from collections import defaultdict
import json

from main import global_ether_id


# LOAD DATA

def load_trades(file_csv):
    trades = pd.read_csv(file_csv)
    print(f"Info: read file {file_csv} as DataFrame with {len(trades)} rows.")
    print(f"Columns are: {', '.join(trades.columns)}")
    return trades


def get_successful_and_complete_trades(trades, status_column=None, status_success=None):
    n = len(trades)
    if status_column and status_success:
        trades = trades[trades[status_column] == status_success]
    trades = trades.dropna()
    print(f"Info: dropped {n - len(trades)} rows with missing/unsuccessful status. {len(trades)} rows remaining.")
    return trades


def get_ether_token_trades(trades, token_column1, token_column2):
    n = len(trades)
    trades = trades[(trades[token_column1] == global_ether_id) | (trades[token_column2] == global_ether_id)]
    trades = trades[trades[token_column1] != trades[token_column2]]
    print(f"Info: dropped {n - len(trades)} rows between two tokens or same currency trades. {len(trades)} rows remaining.")
    return trades


def merge_trades_with_daily_usd_price(trades, price_file_csv="data/EtherDollarPrice.csv"):
    ether_dollar = pd.read_csv(price_file_csv)
    ether_dollar.columns = ["date", "timestamp", "dollar"]
    ether_dollar['date'] = pd.to_datetime(ether_dollar['date'], format='%m/%d/%Y')

    # Add timestamp of date to trades for merging
    min_trades_timestamp = trades['timestamp'].min()
    max_trades_timestamp = trades['timestamp'].max()

    # Get greatest Dollar timestamp that is smaller-equal than the smallest trades timestamp
    min_dollar_timestamp = ether_dollar[ether_dollar['timestamp'] <= min_trades_timestamp].sort_values('timestamp').iloc[-1]['timestamp']

    # Get smallest Dollar timestamp that is greater-equal than the greatest trades timestamp
    max_dollar_timestamp = ether_dollar[ether_dollar['timestamp'] >= max_trades_timestamp].iloc[0]['timestamp']

    # Get left sides of intervals
    intervals_left = ether_dollar[(ether_dollar['timestamp'] >= min_dollar_timestamp) & 
                                  (ether_dollar['timestamp'] <= max_dollar_timestamp)]['timestamp']

    # Cut IDEX timestamps based on intervals
    trades['cut'] = pd.cut(trades['timestamp'], bins=intervals_left, labels=intervals_left[:-1], 
                           include_lowest=True, right=False)
    trades['cut'] = trades['cut'].astype(float)

    # Merge buy eth trades with eth-dollar price
    trades_buyeth = trades[trades['tokenBuy'] == global_ether_id]
    trades_buyeth = pd.merge(trades_buyeth, ether_dollar[['timestamp', 'dollar', 'date']], 
                             left_on='cut', right_on='timestamp', suffixes=('', '_y'))
    trades_buyeth = trades_buyeth.rename(columns={'dollar': 'eth_price'})
    trades_buyeth = trades_buyeth[['date', 'cut', 'blockNumber', 'timestamp', 'transactionHash',
                                   'maker', 'taker', 'tokenBuy', 'tokenSell', 'amountBoughtReal',
                                   'amountSoldReal', 'price', 'feeMake', 'feeTake', 'eth_price']]
    trades_buyeth = trades_buyeth.rename(columns={
        'maker': 'eth_buyer', 'taker': 'eth_seller', 'tokenBuy': 'ether', 'tokenSell': 'token',
        'amountBoughtReal': 'trade_amount_eth', 'amountSoldReal': 'trade_amount_token',
        'feeMake': 'fee_eth_buyer', 'feeTake': 'fee_eth_seller'
    })
    trades_buyeth['trade_amount_dollar'] = trades_buyeth['trade_amount_eth'] * trades_buyeth['eth_price']
    trades_buyeth['token_price_in_eth'] = 1 / trades_buyeth['price']

    # Merge sell eth trades with eth-dollar price
    trades_selleth = trades[trades['tokenSell'] == global_ether_id]
    trades_selleth = pd.merge(trades_selleth, ether_dollar[['timestamp', 'dollar', 'date']], 
                              left_on='cut', right_on='timestamp', suffixes=('', '_y'))
    trades_selleth = trades_selleth.rename(columns={'dollar': 'eth_price'})
    trades_selleth = trades_selleth[['date', 'cut', 'blockNumber', 'timestamp', 'transactionHash',
                                     'maker', 'taker', 'tokenSell', 'tokenBuy', 'amountSoldReal',
                                     'amountBoughtReal', 'price', 'feeMake', 'feeTake', 'eth_price']]
    trades_selleth = trades_selleth.rename(columns={
        'taker': 'eth_buyer', 'maker': 'eth_seller', 'tokenSell': 'ether', 'tokenBuy': 'token',
        'amountSoldReal': 'trade_amount_eth', 'amountBoughtReal': 'trade_amount_token',
        'feeTake': 'fee_eth_buyer', 'feeMake': 'fee_eth_seller'
    })
    trades_selleth['trade_amount_dollar'] = trades_selleth['trade_amount_eth'] * trades_selleth['eth_price']
    trades_selleth['token_price_in_eth'] = trades_selleth['price']

    # Concatenate and sort
    trades_eth = pd.concat([trades_buyeth, trades_selleth]).sort_values('blockNumber')

    return trades_eth



def merge_EtherDelta_trades_with_daily_usd_price(trades, price_file_csv="data/EtherDollarPrice.csv"):
    ether_dollar = pd.read_csv(price_file_csv)
    ether_dollar.columns = ["date", "timestamp", "dollar"]
    ether_dollar['date'] = pd.to_datetime(ether_dollar['date'], format="%m/%d/%Y")

    # Add timestamp of date to trades for merging
    min_trades_timestamp = trades['timestamp'].min()
    max_trades_timestamp = trades['timestamp'].max()

    # Get greatest Dollar timestamp that is smaller-equal than the smallest trades timestamp
    min_dollar_timestamp = ether_dollar[ether_dollar['timestamp'] <= min_trades_timestamp].sort_values('timestamp').iloc[-1]['timestamp']

    # Get smallest Dollar timestamp that is greater-equal than the greatest trades timestamp
    max_dollar_timestamp = ether_dollar[ether_dollar['timestamp'] >= max_trades_timestamp].iloc[0]['timestamp']

    # Get left sides of intervals
    intervals_left = ether_dollar[(ether_dollar['timestamp'] >= min_dollar_timestamp) & 
                                  (ether_dollar['timestamp'] <= max_dollar_timestamp)]['timestamp']

    # Cut IDEX timestamps based on intervals
    trades['cut'] = pd.cut(trades['timestamp'], bins=intervals_left, labels=intervals_left[:-1], include_lowest=True, right=False)
    trades['cut'] = trades['cut'].astype(float)

    # Merge buy eth trades with eth-dollar price
    trades_buyeth = trades[trades['tokenBuy'] == global_ether_id].merge(
        ether_dollar[['timestamp', 'dollar', 'date']].rename(columns={'dollar': 'eth_price'}),
        left_on='cut', right_on='timestamp', suffixes=('', '_y')
    )
    trades_buyeth = trades_buyeth.rename(columns={
        'maker': 'eth_buyer', 'taker': 'eth_seller', 'tokenBuy': 'ether', 'tokenSell': 'token',
        'amountBoughtReal': 'trade_amount_eth', 'amountSoldReal': 'trade_amount_token'
    })
    trades_buyeth['trade_amount_dollar'] = trades_buyeth['trade_amount_eth'] * trades_buyeth['eth_price']
    trades_buyeth['token_price_in_eth'] = 1 / trades_buyeth['price']

    # Merge sell eth trades with eth-dollar price
    trades_selleth = trades[trades['tokenSell'] == global_ether_id].merge(
        ether_dollar[['timestamp', 'dollar', 'date']].rename(columns={'dollar': 'eth_price'}),
        left_on='cut', right_on='timestamp', suffixes=('', '_y')
    )
    trades_selleth = trades_selleth.rename(columns={
        'taker': 'eth_buyer', 'maker': 'eth_seller', 'tokenSell': 'ether', 'tokenBuy': 'token',
        'amountSoldReal': 'trade_amount_eth', 'amountBoughtReal': 'trade_amount_token'
    })
    trades_selleth['trade_amount_dollar'] = trades_selleth['trade_amount_eth'] * trades_selleth['eth_price']
    trades_selleth['token_price_in_eth'] = trades_selleth['price']

    # Concatenate and sort
    trades_eth = pd.concat([trades_buyeth, trades_selleth]).sort_values('blockNumber')

    # Select and reorder columns
    columns = ['date', 'cut', 'blockNumber', 'timestamp', 'transactionHash', 'eth_buyer', 'eth_seller', 
               'ether', 'token', 'trade_amount_eth', 'trade_amount_dollar', 'trade_amount_token', 'token_price_in_eth']
    trades_eth = trades_eth[columns]

    return trades_eth


# SELF TRADES

def filter_self_trades(trades, save=True, folder="output", filename="self_trades"):
    self_trades = trades[trades['eth_buyer'] == trades['eth_seller']]
    non_self_trades = trades[trades['eth_buyer'] != trades['eth_seller']]
    print(f"Info: filtered {len(self_trades)} self-trades. {len(non_self_trades)} non-self-trades remaining.")
    if save:
        filename = filename.split(".")[0] 
        self_trades.to_csv(f"{folder}/{filename}.csv", index=False)
    return {'self_trades': self_trades, 'non_self_trades': non_self_trades}



def summarize_self_trades(self_trades, save=True, folder="output", filename="self_trades_summary"):
    summary = self_trades.groupby(['eth_buyer', 'token']).agg({
        'trade_amount_eth': 'sum',
        'trade_amount_dollar': 'sum',
        'trade_amount_token': 'sum',
        'date': ['min', 'max'],
        'transactionHash': 'count'
    })
    summary = summary.sort_values([('date', 'min')]).reset_index()
    summary.columns = ['trader', 'token', 'tx_sum_eth', 'tx_sum_dollar', 'tx_sum_token', 'start_date', 'end_date', 'tx_count']
    if save:
        filename = filename.split(".")[0] 
        summary.to_csv(f"{folder}/{filename}.csv", index=False)
    return summary


def add_trader_hashes(trades, trader_hashes):

    if trader_hashes.empty:
        trader_addresses = sorted(set(trades['eth_buyer']).union(set(trades['eth_seller'])))
        trader_hashes = pd.DataFrame({
            'trader_address': trader_addresses,
            'trader_id': range(1, len(trader_addresses) + 1)
        })
    else:
        additional_traders = set(trades['eth_buyer']).union(set(trades['eth_seller'])) - set(trader_hashes['trader_address'])
        if additional_traders:
            n_old = len(trader_hashes)
            new_traders = pd.DataFrame({
                'trader_address': sorted(additional_traders),
                'trader_id': range(n_old + 1, n_old + len(additional_traders) + 1)
            })
            trader_hashes = pd.concat([trader_hashes, new_traders], ignore_index=True)
    
    trades = trades.merge(trader_hashes.rename(columns={'trader_address': 'eth_buyer', 'trader_id': 'eth_buyer_id'}), on='eth_buyer', how='left')
    trades = trades.merge(trader_hashes.rename(columns={'trader_address': 'eth_seller', 'trader_id': 'eth_seller_id'}), on='eth_seller', how='left')
    trades = trades.sort_values('timestamp')
    return trades, trader_hashes



def get_address_clusters(trades, global_scc_traders_map, global_trader_hashes, scc_ids, save=True, folder="output", filename="address_clusters"):
    address_clusters = {}

    # for each SCC
    for scc_id in scc_ids:
        scc_traders = global_scc_traders_map.get(scc_id, []) 
        trader_addresses = global_trader_hashes[global_trader_hashes['trader_id'].isin(scc_traders)]['trader_address'].tolist()
        address_clusters[str(scc_id)] = trader_addresses

    # save file
    if save:
        filename = filename.split('.')[0]
        with open(f"{folder}/{filename}.json", "w") as outfile:
            json.dump(address_clusters, outfile)

    return address_clusters

