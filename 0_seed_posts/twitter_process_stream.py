import numpy as np
import json
import gzip
import glob
import os
import clean_text as ct


#-------- Global Variables -------- #
path = '../data'
twitter_path= f'{path}/twitter'
stream_path = f'{twitter_path}/raw/stream'

# outfolder
outfolder = f'{twitter_path}/raw/seed_summaries'

def start_outfile(index):
    outfile = f'{outfolder}/seed_summary_{index}.txt'
    
    # write on creation
    with open(outfile, 'w') as fp:
        fp.write('\t'.join(['tweet_id',
                            'conversation_id',
                            'topic']) + '\n') 
        
    return outfile

def process_folder(pathname, term_to_topic):
    index = 0
    
    outfile = start_outfile(index)
    
    for i, filename in enumerate(glob.glob(f'{pathname}/*')):
        if i%500==0:
            print(f'   Processing file {i}...')
            
            index += 1
            outfile = start_outfile(index)
            
        with gzip.open(filename) as fp:
            try:
                # load tweets
                tweets = json.loads(fp.read())

                # go through each tweet
                for tweet in tweets['data']:
                    tweet_id = tweet['id']
                    conv_id = tweet['conversation_id'] 

                    # get text and remove linebreaks, tabs, etc
                    text = tweet['text']
                    text = ct.get_clean_text(text)

                    # match tokens
                    matches, lists_matched = ct.match_text(text, term_to_topic)
                    
                    if lists_matched:
                        lists = ' | '.join(lists_matched)
                    else:
                        lists = 'None'

                    # write to file
                    if lists != 'None':
                        with open(outfile, 'a') as fp:
                            fp.write(tweet_id + '\t' + conv_id + '\t' + lists + '\n')

            except KeyError:
                pass

            except EOFError:
                print(f'Unable to open {filename}')
                
            except:
                print('Unknown error')
                
    print(f'{i} files processed for {pathname}')


if __name__ == "__main__":

    print('Loading keywords...')
    term_to_topic = ct.load_keywords()

    print('Processing stream files...')    
    process_folder(stream_path, term_to_topic)