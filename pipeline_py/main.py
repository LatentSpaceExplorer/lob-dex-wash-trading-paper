import os
import pandas as pd

import utils
from args import parse_arguments
from scc import detect_scc_for_tokens_layered, get_relevant_scc_by_threshold
from wtd import detect_and_label_wash_trades_for_scc_using_multiple_passes, get_summary_of_wash_trades_per_scc_and_timewindow


global_ether_id = "0x0000000000000000000000000000000000000000"


def main():

    args = parse_arguments()

    wash_window_sizes_args = [int(args.washwindowsizesecondspass1)]
    if args.washwindowsizesecondspass2 is not None:
        wash_window_sizes_args.append(int(args.washwindowsizesecondspass2))
    if args.washwindowsizesecondspass3 is not None:
        wash_window_sizes_args.append(int(args.washwindowsizesecondspass3))

    pipeline(trades_file=args.trades,
                        prices_file=args.prices,
                        dex_type=args.dex,
                        output_folder=args.output,
                        scc_threshold_rank=args.sccthresholdrank,
                        wash_trade_detection_ether=args.washdetectionether,
                        wash_trade_detection_margin=args.margin,
                        wash_window_sizes_seconds=wash_window_sizes_args)



def pipeline(trades_file, 
                      prices_file,
                      dex_type,
                      output_folder,
                      scc_threshold_rank=100,
                      wash_trade_detection_ether=True,
                      wash_trade_detection_margin=0.1,
                      wash_window_sizes_seconds=[60*60*24*7]):
    
    os.makedirs(output_folder, exist_ok=True)

    # Initialize variables
    global_trader_hashes = pd.DataFrame(columns=['trader_address', 'trader_id'])
    global_scc_traders_map = {}


    # Load and prepare trades
    trades = utils.load_trades(trades_file)

    # Merge with USD price
    if dex_type == "IDEX":
        trades = utils.get_successful_and_complete_trades(trades, 'status', 1)
        trades = utils.get_ether_token_trades(trades, 'tokenBuy', 'tokenSell')
        trades = utils.merge_trades_with_daily_usd_price(trades, prices_file)
    else:  # EtherDelta
        trades = utils.get_successful_and_complete_trades(trades)
        trades = utils.get_ether_token_trades(trades, 'tokenBuy', 'tokenSell')
        trades = utils.merge_EtherDelta_trades_with_daily_usd_price(trades, prices_file)


    # Filter self trades
    l = utils.filter_self_trades(trades, True, output_folder)
    utils.summarize_self_trades(l['self_trades'], True, output_folder)
    trades = l['non_self_trades']


    # Add trader hashes
    trades, trader_hashes = utils.add_trader_hashes(trades, global_trader_hashes)
    global_trader_hashes = trader_hashes

    # Detect SCC
    scc_dt = detect_scc_for_tokens_layered(trades, global_scc_traders_map, save=True, folder=output_folder)
    relevant_scc_ids = get_relevant_scc_by_threshold(scc_dt, scc_threshold_rank)

    # Detect and label wash trades
    wash_trades, trades_labeled = detect_and_label_wash_trades_for_scc_using_multiple_passes(
        trades, global_scc_traders_map, relevant_scc_ids, wash_window_sizes_seconds, 
        ether=wash_trade_detection_ether, margin=wash_trade_detection_margin, 
        save=True, folder=output_folder)
    
    # Summarize wash trades (only for EtherDelta in the original, but can be useful for both)
    wash_trades_multiple_passes_summary = get_summary_of_wash_trades_per_scc_and_timewindow(
        wash_trades, 'multiple_windows', multiple_passes=True, save=True, folder=output_folder)

    # Get address clusters
    utils.get_address_clusters(trades, global_scc_traders_map, global_trader_hashes, relevant_scc_ids, 
                         save=True, folder=output_folder)



if __name__ == "__main__":
    main()