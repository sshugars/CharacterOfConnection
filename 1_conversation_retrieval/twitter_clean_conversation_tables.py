import numpy as np
import json
import gzip
import glob
import os
import pandas as pd
import clean_text as ct

# data 
path = '../data'
twitter_path= f'{path}/twitter'

collected_convo_path = f'{twitter_path}/conversation_tables'
seed_locations = f'{twitter_path}/raw/seed_locations'

# cols that should be loaded as strings
str_cols = ['tweet_id', 'conversation_id', 'author', 
            'parent_id', 'parent_author', 'quote_id']

dtypes = dict((col, str) for col in str_cols)

term_to_topic = ct.load_keywords(verbose=False)

def process_tweet(tweet):
    conv_id = tweet['conversation_id'] 
    author = tweet['author_id']
    created_at = tweet['created_at']

    # get text and remove linebreaks, tabs, etc
    text = tweet['text']
    text = ct.get_clean_text(text)

    # match tokens
    matches, lists_matched = ct.match_text(text, term_to_topic)

    # check if parent tweet
    parent_id = np.nan
    quote_id = np.nan

    try:
        for item in tweet['referenced_tweets']:
            if item['type'] == 'replied_to':
                parent_id = item['id']
            elif item['type'] == 'quoted':
                quote_id = item['id']
    except KeyError:
        pass

    # author of parent tweet
    try:
        in_reply_to = tweet['in_reply_to_user_id']
    except KeyError:
        in_reply_to = np.nan

    data_dict = {
        'conversation_id' : conv_id,
        'author' : author,
        'parent_id': parent_id,
        'parent_author' : in_reply_to,
        'quote_id': quote_id,
        'text': text,
        'created_at': created_at,
        'topics': ' | '.join(lists_matched),
        'matched_words': ' | '.join(matches)
    }

    for topic in set(term_to_topic.values()):
        if topic in lists_matched:
            data_dict[topic] = 1
        else:
            data_dict[topic] = 0
            
    return data_dict


def find_missing(tweet_id, search_file, includes):
    
    # open file
    with gzip.open(search_file, 'r') as fp:
        data = json.loads(fp.read())
        data_dict = dict()
        
        if includes == 0:
            tweets = data['data']
        else:
            tweets = data['includes']['tweets']
            
        # search tweets
        for tweet in tweets:
            found_id = tweet['id']
            
            # if this is the tweet we're looking for
            if found_id == tweet_id:
                data_dict = process_tweet(tweet)
                
                # add tweet_id
                data_dict['tweet_id'] = tweet_id
                
    return data_dict


def process_file(filename):
    outfile = filename
    conversation_id = filename.split('/')[-1].split('.')[0]
    
    # load data
    df = pd.read_csv(filename, sep='\t',
                     dtype=dtypes,
                     quoting=3)
    
    # check for additional seed data
    seed_file = f'{seed_locations}/{conversation_id}_seeds.txt'
    new = 0
    
    if os.path.exists(seed_file):
        
        # tweets we already have data for
        found_ids = set(df['tweet_id'])
        
        # check seeds
        with open(seed_file, 'r') as fp:
            for i, line in enumerate(fp.readlines()):
                if i > 0:
                    line = line.strip().split('\t')

                    tweet_id = line[0]

                    # we don't already have this tweet
                    if tweet_id not in found_ids:
                        search_file = line[1]
                        includes = line[2]

                        data_dict = find_missing(tweet_id, search_file, includes)

                        if len(data_dict) > 0:
                            new += 1
                            
                            # put data in correct order
                            new_data = [data_dict[key] for key in df.columns]

                            # add to dataframe
                            df.loc[len(df.index)] = new_data
        
    # drop duplicates
    clean = df.drop_duplicates(ignore_index=True)
    
    # overwrite old file
    clean.to_csv(outfile, sep='\t', index=False)
    
    return new

if __name__ == "__main__":
    
    added = 0
    
    for i, filename in enumerate(glob.glob(f'{collected_convo_path}/*')):
        if i%500 == 0:
            print(f'   Processing file {i}. {added} tweets added so far')
            
        added += process_file(filename)
        
    print(f'Total of {added} new tweets found.')