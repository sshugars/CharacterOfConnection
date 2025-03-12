import json
import os
import urllib.request
import glob
from datetime import datetime, timedelta
import time

# Global variables
reddit_path = '../data/Reddit/raw'
comment_path = f'{reddit_path}/comment_search'
thread_path = f'{reddit_path}/threads'

comment_record = f'{reddit_path}/seed_comments.txt'
bad_file = f'{reddit_path}/unretrievable_threads.txt'

    
def get_bad_threads(bad_file):
    bad = set()
    
    if os.path.exists(bad_file):    
        with open(bad_file, 'r') as fp:
            for line in fp.readlines():
                line = line.strip().split('\t')
                
                bad.add(line[0])
                
    print(f'{len(bad)} threads returned errors.')
    return bad

def get_seed_threads(comment_record):
    seeds = dict()
    
    with open(comment_record, 'r') as fp:
        for i, line in enumerate(fp.readlines()):

            # if not header:
            if i > 0:
                line = line.strip().split('\t')
                thread_id = line[1]
                keyword = line[2]
                thread = '/'.join(line[3].split('/')[:-2])
                
                seeds[thread_id] = [keyword, thread]
            
    print(f'{len(seeds)} thread_ids initially found.')
    return seeds
    
def get_found(thread_path):
    # threads retreived
    found = set()

    for i, filename in enumerate(glob.glob(f'{thread_path}/*')):
        thread_id = filename.split('_')[-1].split('.')[0]
        found.add(thread_id)

    print(f'{len(found)} threads already retrieved')
    
    return found

if __name__ == "__main__":

    seeds = get_seed_threads(comment_record)
    found = get_found(thread_path)
    bad = get_bad_threads(bad_file)

    total_searched = found.union(bad)
    
    missing = dict()
    
    for thread_id, details in seeds.items():
        if thread_id not in total_searched:
            missing[thread_id] = details

    print(f'{len(missing)} threads remaining to search.')
    
    
    for thread_id, details in missing.items():
        keyword = details[0]
        thread = details[1]
        
        url = f'https://www.reddit.com{thread}.json'
        outfile = f'{thread_path}/{keyword}_{thread_id}.json'

        trying = True
        print(f'{datetime.now()}: Retrieving conversation: {url}')

        while trying:
            try:
                response = urllib.request.urlopen(url)
                data = json.loads(response.read())

                with open(outfile, 'w') as fp:
                    fp.write(json.dumps(data))

                # conversation sucessfully searched
                trying = False

                # sleep 5 seconds    
                time.sleep(5)

            except urllib.error.URLError as e:
                print('   ', e.code, e.reason)  
                if e.code == 429:
                    print(f'   Sleeping for 30 seconds.')
                    time.sleep(30)

                # keep trying as True

                # any other error, write to file
                else:
                    with open(bad_file, 'a') as fp:
                        fp.write(thread_id + '\t' + str(e.code) + '\t' + e.reason + '\n') 

                    # conversation cannot be retrieved
                    trying = False


            except UnicodeEncodeError:
                print(f'   Encoding error, skipping URL {url}')

                # write to file so we don't do this again
                with open(bad_file, 'a') as fp:
                    fp.write(thread_id + '\tBadJson\tEncoding Error\n')

                # conversation cannot be retrieved
                trying = False

    print(f'All conversations searched.')