# import needed packages
import requests
import json
import os
import time
import gzip
from datetime import datetime, timedelta

# set up Twitter authorization
from auth import TwitterAuth
headers = TwitterAuth.headers

# Global variable: topic searching
twitter_path = '../../data/twitter'
out_path = f'{twitter_path}/raw/conversations'

# make sure outpath exists
if not os.path.exists(out_path):
    os.mkdir(out_path)


# Helper function to load id file
def load_ids(filename):
    ids = set()

    with open(filename, 'r') as fp:
        for line in fp.readlines():
            ids.add(line.strip())
            
    return ids


# get set of conversation ids to search
def get_search_convos(to_search_file, been_searched_file):
    to_search = load_ids(to_search_file)
    print(f'Started with {len(to_search)} conversations.')

    if os.path.exists(been_searched_file):
        been_searched = load_ids(been_searched_file)
        print(f'{len(been_searched)} conversations searched.')

        ids = to_search - been_searched
        print(f'{len(ids)} conversations remaing.')

    else:
        ids = to_search

    print()
    
    return ids

def check_status(response):
    # too many requests
    if response.status_code == 429:
        # check usage cap
        try:
            usage_cap_error = 'Usage cap exceeded: Monthly product cap'
            rep_check = response.json().get('detail')

            if rep_check == usage_cap_error:
                print('Monthly Cap Exceeded!')
                raise Exception(
                    "Request returned an error: {} {}".format(
                        response.status_code, response.text
                    )
                )
        except:
            print(f'  Too many requests (429). Sleeping for 15 minutes stating at {datetime.now()}')
            time.sleep(15*60) 
   
    # service unavailable
    elif response.status_code == 503:
        print(f'  Service unavailable (503). Sleeping for 1 minute stating at {datetime.now()}')
        time.sleep(60) 

    else:
        print(f'Unknown error. status_code {response.status_code}.')


# connect to Twitter endpoint
def connect_to_endpoint(url, headers, params=dict()):
    try:
        response = requests.request("GET", url, headers=headers, params=params)
        time.sleep(1)

        while response.status_code != 200:
            check_status(response)
            response = requests.request("GET", url, headers=headers, params=params)
            time.sleep(1)

    except:
        print('Timeout error, sleeping for 5 seconds...')
        time.sleep(5) 

        response = connect_to_endpoint(url, headers, params)

     
    return response



# initialize parameters for search
def init_params():
    # tweet fields to retrieve 
    # full list of available fields at:
    # https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
    fields = ['id', 'conversation_id', 'text',
              'author_id', 'in_reply_to_user_id',
              'referenced_tweets',
              'attachments', 'context_annotations', 'entities',
              'public_metrics', 'created_at', 'lang', 'geo']



    expansions = ['author_id', 
                  'entities.mentions.username', 
                  'geo.place_id', 'in_reply_to_user_id', 
                  'referenced_tweets.id', 
                  'referenced_tweets.id.author_id']

    user_fields = ['id', 'name', 'username',
                  'created_at', 'description', 'entities',
                  'location', 'pinned_tweet_id',
                  'protected', 'url', 'verified',
                  'public_metrics']



    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    params = {'tweet.fields': ','.join(fields),
              'user.fields': ','.join(user_fields),
              'expansions': ','.join(expansions),
              'start_time': '2018-01-25T00:00:00Z', # need start time for it to work!
              'max_results': 100
             }

    return params


# load ids and search for each one
def search(url):
    to_search_file = f'{twitter_path}/final_sample_convos.txt'
    been_searched_file = f'{twitter_path}/conversations_searched.txt'
    
    ids = get_search_convos(to_search_file, been_searched_file)
    
    for i, convo in enumerate(ids):
        if i%100==0:
            print(f'***********{i} Conversations Searched*************')

        # initialize for each conversation
        next_token = -1
        searching = True
        total_tweets = 0

        # set parameters
        params = init_params()
        params['query'] = 'conversation_id:{}'.format(convo)

        # search for tweets
        while searching:
            index = 0
            filename = '{}_{}.json.gzip'.format(convo, index)
            outfile = f'{out_path}/{filename}'

            while os.path.exists(outfile):
                index += 1
                filename = '{}_{}.json.gzip'.format(convo, index)
                outfile = f'{out_path}/{filename}'

            start = datetime.now()

            # add pagination if it exists
            if next_token != -1:
                params['next_token'] = next_token

            # search for tweets
            response = connect_to_endpoint(url, headers, params)
            json_response = response.json()

            # archive search restricts to 1 search per second
            time.sleep(1)

            # write response to file
            with gzip.open(outfile, 'w') as fp:
                fp.write((json.dumps(json_response) + '\n').encode())

            if 'data' in json_response:
                total_tweets += len(json_response['data'])

            # get next token
            try:
                next_token = json_response['meta']['next_token']

            except:
                searching = False

            # check how long it's been since we started this loop
            end = datetime.now()

            diff = end - start

            if diff.seconds > 25*60: # if we've been waiting more than 25 min
                searching = False

        # save list of conversations we have searched
        with open(f'{twitter_path}/conversations_searched.txt', 'a') as fp:
            fp.write(convo + '\n')

        # print what we've found for this conversation
        print('found {} tweets for conversation {}'.format(total_tweets, convo))


if __name__ == "__main__":
    url = 'https://api.twitter.com/2/tweets/search/all'
    search(url)