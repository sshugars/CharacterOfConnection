import numpy as np
import json
import gzip
import glob
import os

#-------- Global Variables -------- #
path = '../data'
twitter_path= f'{path}/twitter'
stream_path = f'{twitter_path}/raw/stream'

# outfolder
outfolder = f'{twitter_path}/raw/seed_locations'

def start_outfile(outfile):
    
    # write on creation
    with open(outfile, 'w') as fp:
        fp.write('\t'.join(['tweet_id',
                            'filename',
                            'includes']) + '\n') 


def process_folder(pathname):    
    for i, filename in enumerate(glob.glob(f'{pathname}/*')):
        if i%500==0:
            print(f'   Processing file {i}...')
            
        with gzip.open(filename) as fp:
            try:
                # load tweets
                tweets = json.loads(fp.read())

                # go through each tweet
                for tweet in tweets['data']:
                    tweet_id = tweet['id']
                    convo_id = tweet['conversation_id'] 
                    
                    outfile = f'{outfolder}/{convo_id}_seeds.txt'
                    
                    if not os.path.exists(outfile):
                        start_outfile(outfile)
                   
                    # for append data to convo_file
                    # 0 to indicate not in includes
                    with open(outfile, 'a') as fp:
                        fp.write(tweet_id + '\t' + filename + '\t' + '0' + '\n')
                        
                        
                # go through all includes
                for tweet in tweets['includes']['tweets']:
                    tweet_id = tweet['id']
                    convo_id = tweet['conversation_id'] 
                    
                    outfile = f'{outfolder}/{convo_id}_seeds.txt'
                    
                    if not os.path.exists(outfile):
                        start_outfile(outfile)
                   
                    # for append data to convo_file
                    # 1 to indicate in includes
                    with open(outfile, 'a') as fp:
                        fp.write(tweet_id + '\t' + filename + '\t' + '1' + '\n')


            except KeyError:
                pass

            except EOFError:
                print(f'Unable to open {filename}')
                
            except:
                print('Unknown error')
                
    print(f'{i} files processed for {pathname}')


if __name__ == "__main__":

    print('Processing stream files...')    
    process_folder(stream_path)