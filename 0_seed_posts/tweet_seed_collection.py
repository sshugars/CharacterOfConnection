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

# --------- Global Variables --------------#s

path = '../data/twitter'

keyword_file = 'keywords.txt'
convo_id_file = '{}/convo_ids.txt'.format(path)
user_id_file = '{}/user_ids.txt'.format(path)
raw_path = '{}/raw/stream'.format(path)


# --------- Functions to search Twitter -------- #
# Tweet fields are adjustable.
# Options include:
# attachments, author_id, context_annotations,
# conversation_id, created_at, entities, geo, id,
# in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
# possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
# source, text, and withheld

def connect_to_endpoint(url, headers, params=dict()):
    response = requests.request("GET", url, headers=headers, params=params)

    if response.status_code == 200:
        # print('API request successful.')
        pass
    
    # too many requests
    elif response.status_code == 429:

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
            
            response = requests.request("GET", url, headers=headers, params=params)

    # service unavailable
    elif response.status_code == 503 or response.status_code == 500:
        print(f'  Service unavailable (503). Sleeping for 1 minute stating at {datetime.now()}')
        time.sleep(60) 
        
        response = requests.request("GET", url, headers=headers, params=params) 
        
    else:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def load_keywords(keyword_file):
    keywords = list()

    with open(keyword_file) as fp:
        for line in fp.readlines():
            line = line.strip()
            if line != '':
                keywords.append(line.strip())

    print(f'{len(keywords)} keywords found.')
    return keywords


def init_params():

    # load keywords
    keywords = load_keywords(keyword_file)

    # tweet fields to retrieve 
    # full list of available fields at:
    # https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
    fields = ['id', 'conversation_id', 'text',
              'author_id', 'in_reply_to_user_id',
              'referenced_tweets',
              'attachments', 'context_annotations', 'entities',
              'public_metrics', 'created_at', 'lang', 'geo']

    expansions = ['attachments.poll_ids', 
                  'attachments.media_keys', 
                  'author_id', 
                  'entities.mentions.username', 
                  'geo.place_id', 'in_reply_to_user_id', 
                  'referenced_tweets.id', 
                  'referenced_tweets.id.author_id']

    language = ' lang:en'

    query = ' OR '.join(keywords) + ' -is:retweet' + language

    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    params = {'query': query,
              'tweet.fields': ','.join(fields),
              'max_results': '100',
              'since_id': '1597202830011817985',
#             'start_time':  '2021-05-03T01:00:00-00:00',
              'expansions': ','.join(expansions)}

    return params

if __name__ == "__main__":

    # url for recent tweets
    url = 'https://api.twitter.com/2/tweets/search/recent'
    params = init_params()


    # loop for searching
    next_token = -1
    searching = True
    total_tweets = 0

    print('Beginning search...')
    
    while searching:
        start = datetime.now()

        if next_token != -1:
            params['next_token'] = next_token

        # search for tweets
        json_response = connect_to_endpoint(url, headers, params)
        try:
            total_tweets += len(json_response['data'])
        except:
            pass

        # write to file
        t = time.localtime()
        file_time = time.strftime('%Y-%m-%d_%H-%M-%S', t)

        with gzip.open('{}/{}.json.gzip'.format(raw_path, file_time), 'w') as fp:
            fp.write((json.dumps(json_response) + '\n').encode()) 

        # get next token
        try:
            next_token = json_response['meta']['next_token']
          
        except:
            searching = False

        # check how long it's been since we started this loop
        end = datetime.now()

        diff = end - start

        if diff.seconds > 25*60: # if we've been waiting more than 25 min
            searching=False

          
    print('{} total tweets retrieved'.format(total_tweets))
