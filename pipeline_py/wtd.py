import os
import numpy as np
import polars as pl
import numpy as np
from tqdm import tqdm



def detect_label_wash_trades(df: pl.DataFrame, margin: float = 0.1) -> pl.DataFrame:
    buyers = df['buyer'].to_list()
    sellers = df['seller'].to_list()
    amounts = df['amount'].to_list()

    balance_map = {}
    trade_amounts = []

    for buyer, seller, amount in zip(buyers, sellers, amounts):
        trade_amounts.append(amount)
        balance_map[buyer] = balance_map.get(buyer, 0) + amount
        balance_map[seller] = balance_map.get(seller, 0) - amount

    for idx in range(len(df) - 1, 0, -1):
        balances = np.array(list(balance_map.values()))
        mean_trade_vol = np.mean(trade_amounts)
        if mean_trade_vol == 0:
            mean_trade_vol = 1
        balances = np.abs(balances / mean_trade_vol)

        if np.all(balances <= margin):
            wash_indices = list(range(idx + 1))
            df = df.with_columns(
                pl.when(pl.col('wash_label').is_null())
                .then(pl.lit(False))
                .otherwise(pl.col('wash_label'))
                .alias('wash_label')
            )
            df = df.with_columns(
                pl.when(pl.arange(0, len(df)).is_in(wash_indices))
                .then(pl.lit(True))
                .otherwise(pl.col('wash_label'))
                .alias('wash_label')
            )
            return df

        amount = amounts[idx]
        trade_amounts.pop()

        balance_map[buyers[idx]] -= amount
        balance_map[sellers[idx]] += amount

    return df



def detect_and_label_wash_trades_for_scc_using_multiple_passes(
    trades, global_scc_traders_map, relevant_scc, window_sizes_in_seconds, window_start=None, 
    ether=True, margin=0.1, save=True, folder="output", 
    filename="wash_trades_multiple_windows"
):   
    print(f"Starting wash trade labeling with {len(window_sizes_in_seconds)} passes.")
    
    checked_trades_hashes = []
    checked_trades_labels = []

    # Convert to polars DataFrame
    trades = pl.from_pandas(trades)

    trades = trades.with_columns(pl.lit(None).alias("wash_label"))

    # if window start is not given, take start of first day of given trades
    if window_start is None:
        window_start = trades['cut'].min()

    wash_trades = {}
    
    for window_size in window_sizes_in_seconds:

        # breaks from start to last timestamp (incl.), by given steps in seconds
        intervals = np.arange(window_start, trades['timestamp'].max(), window_size)
        
        for scc_id in tqdm(relevant_scc, desc=f"Processing SCCs for window size {window_size}"):

            scc_traders = global_scc_traders_map[scc_id]
            
            # Filter trades for the relevant SCC
            scc_trades = trades.filter(
                (pl.col("eth_seller_id").is_in(scc_traders)) & 
                (pl.col("eth_buyer_id").is_in(scc_traders)) & 
                ((pl.col("wash_label") == False) | (pl.col("wash_label").is_null()))
            ).sort("cut")
            
            if len(scc_trades) == 0:
                # wash_trades[scc_id] = {str(window_size): []}
                # wash_trades.setdefault(scc_id, {}).setdefault(str(window_size), {})
                continue
            
            # label these trades as FALSE in original trade set to indicate they have been checked
            trades = trades.with_columns(
                pl.when(pl.col("transactionHash").is_in(scc_trades["transactionHash"]))
                .then(False)
                .otherwise(pl.col("wash_label"))
                .alias("wash_label")
            )

            # Prepare trades for processing
            temp_trades = scc_trades.select([
                "transactionHash", "token", "date", "timestamp", "trade_amount_dollar", "wash_label",
                pl.col("eth_buyer" if ether else "eth_seller").alias("buyer"),
                pl.col("eth_seller" if ether else "eth_buyer").alias("seller"),
                pl.col("trade_amount_eth" if ether else "trade_amount_token").alias("amount")
            ])


            # Process trades in time windows
            temp_trades = temp_trades.with_columns(
                pl.col("timestamp").cut(intervals, left_closed=True).alias("interval")
            )

            for names, data in temp_trades.group_by(['token', 'interval']):

                detected_wash_trades = detect_label_wash_trades(data, margin)

                # copy tx_hash and label to checked_trades
                checked_trades_hashes.extend(detected_wash_trades['transactionHash'])
                checked_trades_labels.extend(detected_wash_trades['wash_label'])
 
                wash_trades.setdefault(scc_id, {}).setdefault(str(window_size), {})['.'.join(names)] = detected_wash_trades


            # update trades with checked_trades
            # join checked_trades to trades and replace with wash_label from checked_trades
            checked_trades_df = pl.DataFrame(
                {"transactionHash": checked_trades_hashes,"wash_label": checked_trades_labels}, 
                schema={"transactionHash": pl.Utf8, "wash_label": pl.Boolean}
            )
            try:
                tx_hash_true_list = checked_trades_df.filter(checked_trades_df['wash_label'] == True)['transactionHash'].to_list()
            except Exception as e:
                tx_hash_true_list = []

            trades = (
                trades
                .with_columns(
                    pl.when(pl.col("transactionHash").is_in(tx_hash_true_list))
                    .then(True)
                    .otherwise(pl.col("wash_label"))
                    .alias("wash_label")
                )
            )

            checked_trades_hashes = []
            checked_trades_labels = []

    if save:
        # Save results
        filename = filename.split('.')[0]
        trades.write_csv(os.path.join(folder, "trades_labeled.csv"))
    
    return wash_trades, trades



def get_summary_of_wash_trades_per_scc_and_timewindow(wash_trades, window_size_name, multiple_passes=False, 
                                                      save=True, folder="output", filename="wash_trades_summary"):
    print("Info: producing wash trading summary...")

    rows = []

    if multiple_passes:
        for scc in wash_trades.keys():
            for window_size in wash_trades[scc].keys():
                for w in wash_trades[scc][window_size].keys():

                    temp = w.split('.')
                    token = temp[0]
                    window = temp[1]

                    wash_trades_scc_window = wash_trades[scc][window_size][w].with_columns(
                        pl.col("wash_label").cast(pl.Boolean) # cast wash_label to bool
                    )

                    num_wash = (wash_trades_scc_window['wash_label'] == True).sum()
                    num_all = len(wash_trades_scc_window)
                    amount_wash = wash_trades_scc_window.filter(pl.col.wash_label == True)['amount'].sum()
                    amount_all = wash_trades_scc_window['amount'].sum()
                    amount_dollar_wash = wash_trades_scc_window.filter(pl.col.wash_label == True)['trade_amount_dollar'].sum()
                    amount_dollar_all = wash_trades_scc_window['trade_amount_dollar'].sum()
                    new_row = {'scc_hash': scc, 'token': token, 'window_size': window_size, 
                            'time': window, 'num_wash_trades': num_wash, 'num_trades': num_all, 
                            'total_amount_wash': amount_wash, 'total_amount': amount_all, 
                            'total_amount_dollar_wash': amount_dollar_wash, 
                            'total_amount_dollar': amount_dollar_all}

                    rows.append(new_row)

    
    else:
        for scc in wash_trades.keys():
            for w in wash_trades[scc].keys():

                temp = w.split('.')
                token = temp[0]
                window = temp[1]

                wash_trades_scc_window = wash_trades[scc][w].with_columns(
                    pl.col("wash_label").cast(pl.Boolean) # cast wash_label to bool
                )

                num_wash = (wash_trades_scc_window['wash_label'] == True).sum()
                num_all = len(wash_trades_scc_window)
                amount_wash = wash_trades_scc_window.filter(pl.col.wash_label == True)['amount'].sum()
                amount_all = wash_trades_scc_window['amount'].sum()
                amount_dollar_wash = wash_trades_scc_window.filter(pl.col.wash_label == True)['trade_amount_dollar'].sum()
                amount_dollar_all = wash_trades_scc_window['trade_amount_dollar'].sum()
                new_row = {'scc_hash': scc, 'token': token, #'window_size': window_size, 
                        'time': window, 'num_wash_trades': num_wash, 'num_trades': num_all, 
                        'total_amount_wash': amount_wash, 'total_amount': amount_all, 
                        'total_amount_dollar_wash': amount_dollar_wash, 
                        'total_amount_dollar': amount_dollar_all}

                rows.append(new_row)
    
    wash_trades_dt = pl.DataFrame(rows)


    if save:
        filename = os.path.splitext(filename)[0]
        wash_trades_dt.write_csv(os.path.join(folder, f"{filename}_{window_size_name}.csv"))
    
    return wash_trades_dt