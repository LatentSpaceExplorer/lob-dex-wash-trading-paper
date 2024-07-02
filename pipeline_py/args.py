import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="Run the pipeline for detecting wash trades.")
    
    parser.add_argument('-d', '--dex', type=str, default='IDEX',
                        help="Name of DEX, must be either 'IDEX' or 'EtherDelta' [default=IDEX]")
    parser.add_argument('-t', '--trades', type=str, default='data/IDEXTrades-preprocessed.csv',
                        help="Trade dataset file name [default=data/IDEXTrades-preprocessed.csv]")
    parser.add_argument('-p', '--prices', type=str, default='data/EtherDollarPrice.csv',
                        help="Ether-Dollar-Price file name [default=data/EtherDollarPrice.csv]")
    parser.add_argument('-o', '--output', type=str, default='output_IDEX',
                        help="Output folder name [default=output_IDEX]")
    parser.add_argument('--sccthresholdrank', type=int, default=100,
                        help="Threshold for relevant SCC: rank [default=100]")
    parser.add_argument('--washdetectionether', action='store_true', default=False,
                        help="Should wash trades be detected for Ether amounts (default=False)")
    parser.add_argument('-m', '--margin', type=float, default=0.1,
                        help="Margin of mean left trader position for wash trade detection [default=0.1]")
    parser.add_argument('--washwindowsizesecondspass1', type=int, default=60*60*24*7,
                        help="Wash trade detection window size for first pass in seconds [default=604800]")
    parser.add_argument('--washwindowsizesecondspass2', type=int, default=None,
                        help="Wash trade detection window size for second pass in seconds [default=None]")
    parser.add_argument('--washwindowsizesecondspass3', type=int, default=None,
                        help="Wash trade detection window size for third pass in seconds [default=None]")

    return parser.parse_args()