from collections import Counter

import pandas as pd
import polars as pl
import networkx as nx
from tqdm import tqdm
import hashlib

def detect_scc_for_tokens_layered(trades, global_scc_traders_map, save=True, folder="output", filename="scc"):

    # convert trades to polars
    trades = pl.from_pandas(trades)

    # Get unique tokens
    token_vector = trades['token'].unique()
    results = []
    
    # Iterate through each token
    for token in tqdm(token_vector, desc="Processing tokens"):
        token_trades = trades.filter(pl.col('token') == token)

        edges = Counter((row['eth_buyer_id'], row['eth_seller_id']) for row in token_trades.select(['eth_buyer_id', 'eth_seller_id']).to_dicts())
        
        g = nx.DiGraph()
        for (u, v), w in edges.items(): 
            g.add_edge(u, v, weight=w)

        while g.number_of_nodes() > 0:
            # Find strongly connected components
            sccs = [list(comp) for comp in nx.strongly_connected_components(g) if len(comp) > 1]

            if len(sccs) == 0:
                break
            
            for scc in sccs:
                sorted_members = sorted(scc)
                c_hash = hashlib.md5(','.join(str(sorted_members)).encode()).hexdigest()
                global_scc_traders_map[c_hash] = sorted_members
                results.append(c_hash)

            # Decrease weights by one
            zero_weight_edges = []
            for u, v, d in g.edges(data=True):
                d['weight'] -= 1
                if d['weight'] == 0:
                    zero_weight_edges.append((u, v))
            
            # Remove zero-weight edges and isolated nodes
            g.remove_edges_from(zero_weight_edges)
            g.remove_nodes_from(list(nx.isolates(g)))

    
    # Create DataFrame for results
    scc_df = pd.DataFrame({'scc_hash': results})
    scc_summary = scc_df.groupby('scc_hash').size().reset_index(name='occurrence')
    scc_summary['num_traders'] = scc_summary['scc_hash'].apply(lambda x: len(global_scc_traders_map[x]))
    
    if save:
        # Save the results
        scc_summary.to_csv(f"{folder}/{filename}.csv", index=False)
        
        mapping = pd.DataFrame([(k, v) for k, values in global_scc_traders_map.items() for v in values], columns=['hash', 'trader_id'])
        mapping.to_csv(f"{folder}/{filename}-mapping.csv", index=False)
    
    return scc_summary



def get_relevant_scc_by_threshold(scc_df, threshold):
    relevant_sccs = scc_df[scc_df['occurrence'] >= threshold]
    print(f"Info: Determined {len(relevant_sccs)} unique SCCs to be relevant at threshold {threshold}")
    print(f"Info: Minimum occurrence is {relevant_sccs['occurrence'].min()}")
    return relevant_sccs['scc_hash'].tolist()
