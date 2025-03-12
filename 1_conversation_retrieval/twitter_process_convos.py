import numpy as np
import json
import gzip
import glob
import os
import shutil
import clean_text as ct

'''
NOTE: This script aggregates over mulitple files
the resulting conversation files MAY include a tweet more than once
'''

#-------- Global Variables -------- #
path = '../data'
twitter_path= f'{path}/Twitter'
in_convo_path = f'{twitter_path}/raw/conversations'
out_convo_path = f'{twitter_path}/conversation_tables'

topics = ['russia',
          'midterms',
          'childcare']

def start_file(outfile):
    
    # header for all files
    header = ['tweet_id',
              'conversation_id',
              'author',
              'parent_id',
              'parent_author',
              'quote_id',
              'text',
              'created_at',
              'topics',
              'matched_words'] + topics

    # write on file creation
    with open(outfile, 'w') as fp:
        fp.write('\t'.join(header) + '\n')

def write_to_file(data_dict, outfile):

    line = '\t'.join([str(item) for item in data_dict.values()])

    with open(outfile, 'a') as fp:
        fp.write(line + '\n')


def process_tweets(tweets, outfile):
    
    # go through each tweet
    for tweet in tweets:
        tweet_id = tweet['id']
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
            'tweet_id' : tweet_id,
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

        for topic in topics:
            if topic in lists_matched:
                data_dict[topic] = 1
            else:
                data_dict[topic] = 0
                
        # write to file
        write_to_file(data_dict, outfile)


if __name__ == "__main__":

    print('Loading keywords...')
    term_to_topic = ct.load_keywords()

    # create out folder
    if not os.path.exists(out_convo_path):
        os.mkdir(out_convo_path)

    print('Processing conversations...')
    new = 0
    
    # process conversations
    for i, convo_file in enumerate(glob.glob(f'{in_convo_path}/*')):
        
        if i%1000==0:
            print(f'...Processed {i} files')
        
        convo_id = convo_file.split('/')[-1].split('_')[0]
        outfile = f'{out_convo_path}/{convo_id}.txt'
        
        # if we haven't already processed this conversation
        if not os.path.exists(outfile):
            start_file(outfile)
            new += 1
        
        # process tweets in this file
        with gzip.open(convo_file, 'r') as fp:
            data = json.loads(fp.read())
            
            # collected tweets
            tweets = data['data']
            process_tweets(tweets, outfile)
            
            # collected included tweets
            try:
                tweets = data['includes']['tweets']
                process_tweets(tweets, outfile)
            except:
                pass
            
    print(f'{i} total files processed.')
    print(f'{new} unique conversations found.')