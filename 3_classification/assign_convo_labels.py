import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime


############
# This code is SLOW
# Use groupby instead
##############

path = '../../data'
chunk_folder = f'{path}/classified'
on_topic_file = f'{path}/conversation_to_topic.txt'
off_topic_file = f'{path}/no_topic.txt'

topics = ['Childcare/parenting', 
          'Russia/Ukraine war',
          'US midterm elections']


def get_corpus():
    # load full corpus
    print('Loading full corpus')
    
    dfs = list()

    for filename in glob.glob(f'{chunk_folder}/*'):
        df = pd.read_csv(filename, sep='\t',
                         dtype={'pid':str, 'conversation_id':str},
                         usecols=['pid', 'conversation_id']+topics)

        dfs.append(df)

    corpus = pd.concat(dfs)
    print(f'{len(corpus)} posts loaded')
    
    corpus=corpus.reset_index()
    
    return corpus


if __name__ == "__main__":
    corpus = get_corpus()
    
    # conversations labeled as on topic
    if os.path.exists(on_topic_file):
        topic_df = pd.read_csv(on_topic_file,sep='\t')
        topic_cids = set(topic_df['conversation_id'])
    else:
        topic_cids = set()
        with open(on_topic_file, 'w') as fp:
            fp.write('conversation_id\tclassified_topic\n')
        
    print(f'{len(topic_cids)} conversations categorized')
        
    # conversations labeled as NOT on topic
    off_topic_cids = set()
    
    if os.path.exists(off_topic_file):
        with open(off_topic_file, 'r') as fp:
            for line in fp.readlines():
                off_topic_cids.add(line.strip())
        
    print(f'{len(off_topic_cids)} conversations with no classified topic')
    
    done = topic_cids.union(off_topic_cids)
    
    print(f'{len(done)} total conversations processed')
    
    cids = set(corpus['conversation_id'])
    print(f'{len(cids)} total conversations')
    
    remaining = cids.difference(done)
    print(f'{len(remaining)} conversations remaining')
    
    # trim corpus to just remaining
    corpus = corpus[corpus['conversation_id'].isin(remaining)]
    
    for i, cid in enumerate(remaining):
        if i%1000==0:
            print(f'Processing conversation {i} of {len(remaining)} at {datetime.now()}')
            
        sub = corpus[corpus['conversation_id']==cid]
        stats = sub[topics].agg('sum').to_dict()
    
        if max(stats.values()) > 0:
            topic = max(stats, key=stats.get)

            with open(on_topic_file, 'a') as fp:
                fp.write(f'{cid}\t{topic}\n')
        else:
            with open(off_topic_file, 'a') as fp:
                fp.write(f'{cid}\n')


    print('All conversations processed')