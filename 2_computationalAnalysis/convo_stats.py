import numpy as np
import pandas as pd
import os
import networkx as nx
from datetime import datetime
from collections import Counter

# Global Variables
lda_topics = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
main_topics = ['russia', 'midterms', 'childcare']

# header for all output data    
header = ['conversation_id', 
          'old_topic_spread', 'new_topic_spread',
          'tox_mean', 'tox_median', 'tox_max',
          'size', 'depth', 'breadth', 'connected',
          'n_users', 'most_comments',
          'keyword_topic'] + main_topics



def get_gini(array):
    array = np.sort(array) # values must be sorted
    index = np.arange(1,array.shape[0]+1) # index per array element
    n = array.shape[0] # number of array elements
    
    gini = (np.sum((2 * index - n  - 1) * array)) / (n * np.sum(array)) # Gini coefficient
    
    return gini


def build_network(df, pid_field):
    G = nx.DiGraph()

    for i, row in df[[pid_field, 'parent_id']].iterrows():
        source = row[pid_field]
        target = row['parent_id']
        
        # don't count root (target is na)
        if target == target:
            G.add_edge(source, target)

    ### size
    nodes = list(G.nodes())

    ### breadth
    try:
        max_children = max(dict(G.in_degree()).values())
    except ValueError:
        max_children = 0
        
    ### depth
    paths = nx.all_pairs_shortest_path_length(G)

    depth = 0

    for source, path in paths:
        if len(path) > depth:
            depth = len(path)
            
    # remove self count
    depth -= 1
    
    ### Connected 
    connected = 0
    
    try:
        if nx.is_connected(G.to_undirected()):
            connected = 1
    except nx.NetworkXPointlessConcept:
        pass
        
    network_stats = {
        'size' : len(nodes),
        'depth' : depth,
        'breadth' : max_children,
        'connected' : connected
    }

    return network_stats


def load_platform_data(platform, pid_field):
    # folder with summary tables
    topic_path = f'../../data/{platform}'

    # pid: [topics 0 - 10]
    topic_file = f'{topic_path}/{platform}_topics.txt'

    # pid, conversation_id, text, russia, midterms, parenting
    corpus_file = f'../../data/{platform}/all_{platform}_text.txt'
    
    lda_df = pd.read_csv(topic_file, sep='\t',
                        dtype={pid_field: str})
    
    corpus = pd.read_csv(corpus_file, sep='\t',
                         usecols=[pid_field, 'conversation_id'],
                         dtype={pid_field: str,
                                'conversation_id':str})
    
    data = corpus.merge(lda_df, on=pid_field, how='inner')
    print(f'Data for {len(data)} posts from {len(set(data.conversation_id))} conversations loaded')
    
    return data


def process_conversation(sub, conversation_id, convo_path, pid_field, outfile):

    line_data = {'conversation_id': conversation_id}
    
    ##### topical diversity
    summary = sub[lda_topics].agg(['sum', 'mean'])
    
    max_topic = max(summary.loc['mean'][lda_topics])
    min_topic = min(summary.loc['mean'][lda_topics])
    line_data['old_topic_spread'] = 1 - (max_topic - min_topic)
    
    # gini coeff
    line_data['new_topic_spread'] = get_gini(summary.loc['sum'])

    
    #### load full conversation data
    convo_file = f'{convo_path}/{conversation_id}.txt'
    df = pd.read_csv(convo_file, sep='\t')
    
    # toxicity scores
    line_data['tox_mean'] = np.mean(df['tox'])
    line_data['tox_median'] = np.median(df['tox'])
    line_data['tox_max'] = max(df['tox'])
    
    #### network stats
    network_stats = build_network(df, pid_field)
    
    for k, v in network_stats.items():
        line_data[k] = v
    
    #### user stats
    users = Counter(df['author'])
    line_data['n_users'] = len(users)
    line_data['most_comments'] = users.most_common(1)[0][1]
    
    #### topic statistics
    topics = list()

    for entry in df['topics']:
        try:
            terms = entry.split(' | ')
            
            for term in terms:
                topics.append(term)
                
        except AttributeError:
            topics.append(entry)
            
    topic_count = Counter(df['topics'])
    
    # most common
    line_data['keyword_topic'] = Counter(topics).most_common(1)[0][0]
    
    for topic in main_topics:
        line_data[topic] = topic_count[topic]


    line = [str(line_data[key]) for key in header]
        
    # append each line of data
    with open(outfile, 'a') as fp:
        fp.write('\t'.join(line) + '\n')
    
def load_done(outfile):
    if os.path.exists(outfile):
        done_df = pd.read_csv(outfile, sep='\t')
        done = set(done_df['conversation_id'])
        
    else:
        # write on file creation
        with open(outfile, 'w') as fp:
            fp.write('\t'.join(header) + '\n')   
        
        done = set()
        
    print(f'{len(done)} conversations already done.')
    return done
    
if __name__ == "__main__":
    
    ####### Reddit data #######
    print('Processing Reddit data')
    
    platform = 'reddit'
    pid_field = 'pid'
    convo_path = f'../../../../data/{platform}/active_tables'
    outfile = f'../../data/{platform}/{platform}_statistics.txt'
    
    # check if outfile already exists
    done = load_done(outfile)
    
    # load all platform data
    data = load_platform_data(platform, pid_field)

    # calculate statistics for each conversation
    convo_ids = set(data.conversation_id)
    
    # remove done conversations
    remaining = convo_ids.difference(done)
    print(f'{len(remaining)} conversations remaining')

    for i, conversation_id in enumerate(remaining):
        if i%1000==0:
            print(f'  {i} files processed at {datetime.now()}')
            
        try:
            sub = data[data['conversation_id']==conversation_id]
            process_conversation(sub, conversation_id, convo_path, pid_field, outfile)
        except:
            pass

    print(f'All Reddit statistics written to {outfile}')


    ####### Twitter data #######
    print('Processing Twitter data')
    
    platform = 'twitter'
    pid_field = 'tweet_id'
    convo_path = f'../../../../data/{platform}/conversation_tables'
    outfile = f'../../data/{platform}/{platform}_statistics.txt'
    
    # write on file creation
    with open(outfile, 'w') as fp:
        fp.write('\t'.join(header) + '\n')
    
    # load all platform data
    data = load_platform_data(platform, pid_field)

    # calculate statistics for each conversation
    convo_ids = set(data.conversation_id)

    for i, conversation_id in enumerate(convo_ids):
        if i%1000==0:
            print(f'  {i} files processed at {datetime.now()}')
            
        sub = data[data['conversation_id']==conversation_id]
        process_conversation(sub, conversation_id, convo_path, pid_field, outfile)

    print(f'All Twitter statistics written to {outfile}')
