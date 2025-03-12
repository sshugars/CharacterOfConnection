import pandas as pd
import numpy as np
import json
import csv
import gzip
import glob
import os
import itertools as it

from googleapiclient import discovery
import time


#-------- Global Variables -------- #
path = '../data'
twitter_path= f'{path}/twitter'

convo_path = f'{twitter_path}/conversation_tables'
tox_processed_file = f'{twitter_path}/reference/tox_calculated.txt'


def init_tox_api():
    api_key = ''

    client = discovery.build(
      "commentanalyzer",
      "v1alpha1",
      developerKey=api_key,
      discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
      static_discovery=False,
    )
    
    return client


def get_score(text, client):
    analyze_request = {
      'comment': { 'text': text},
      'requestedAttributes': {'TOXICITY': {}}
    }

    try:
        response = client.comments().analyze(body=analyze_request).execute()
        score = response['attributeScores']['TOXICITY']['summaryScore']['value']  

    except Exception as e:
        s = str(e)
        
        if s.split()[1] == '400':
            score = np.nan

        elif s.split()[1] == '429':
            print(f'Too many requests, sleeping for 10 seconds at {time.asctime(time.localtime())}')
            score = -1
            time.sleep(10)

        else:
            score = -1
            print(f'{s} at {time.asctime(time.localtime())}')
            time.sleep(5)   

    return score


def get_tox(texts, client):
    scores = dict()

    for i, text in texts.items():
        score = -1 

        while score == -1:
            score = get_score(text, client)

        scores[i] = score
        
    return scores


def load_processed(tox_processed_file):
    processed = set()
    
    with open(tox_processed_file, 'r') as fp:
        for line in fp.readlines():
            processed.add(line.strip())
            
    return processed
    

if __name__ == "__main__":
    
    client = init_tox_api()
    
    # cols that should be loaded as strings
    str_cols = ['tweet_id', 'conversation_id', 'author', 
                'parent_id', 'parent_author', 'quote_id']

    dtypes = dict((col, str) for col in str_cols)
    
    processed = load_processed(tox_processed_file)
    print(f'{len(processed)} files already processed...')
    
    new = 0
    skipped = 0

    # process conversation tweets
    for i, filename in enumerate(glob.glob(f'{convo_path}/*')):
        if i%100==0:
            print(f'{i} conversations, {new} new scores at {time.asctime(time.localtime())}.')
          
        # if we haven't already processed this file
        if filename not in processed:
             
            # load data
            df = pd.read_csv(filename,
                             sep='\t',
                             quoting = 3,
                             dtype=dtypes)

            ######## if we have no tox values ########
            if 'tox' not in df.columns:
                df['tox'] = np.nan

            conversation_id = filename.split('/')[-1].split('.')[0]

            if 'tox' in df.columns:
                missing = df[df['tox'].isna()]
                
                # for now, focus on shorter conversations
                # if len(missing) < 10**4:
                print(f'  {len(missing)} scores to fill for {conversation_id}')

                # get toxicity scores
                texts = missing['text']
                scores = get_tox(texts, client)

                df['tox'].fillna(scores, inplace=True)

                # write to file, overwriting old version
                df.to_csv(filename, sep='\t', index=False)
                new += 1

                # write filename to tox processed file
                with open(tox_processed_file, 'a') as fp:
                    fp.write(filename + '\n')
            else:
                skipped += 1

    print(f'{i} total conversatons found.')
    print(f'{new} conversations with updated toxicity.')
    print(f'{skipped} conversations skipped for now.')

 