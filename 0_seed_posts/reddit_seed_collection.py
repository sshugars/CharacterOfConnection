import json
import os
import requests
import re
from datetime import datetime, timedelta
import time


# Global variables
reddit_path = '../data/reddit'
out_path = f'{reddit_path}/raw/comment_search'
subreddit_file = f'{reddit_path}/subreddits.txt'

dt_format = '%Y-%m-%d %H:%M:%S'
url = 'https://api.pushshift.io/reddit/search/comment'

# keyword file
keyword_file = 'keywords_reddit.txt'

def load_keywords(keyword_file):
    keywords = set()
    with open(keyword_file, 'r') as fp:
        for line in fp.readlines():
            keywords.add(line.strip())
            
    print(f'{len(keywords)} keywords found.')

    return list(keywords)


if __name__ == "__main__":

    start = datetime(2022,9,30)
    end = datetime(2023,1,1)
    step = timedelta(hours=6)

    total_steps = (((end - start).days) * 24) / (step.seconds / 60 / 60)
    
    keywords = load_keywords(keyword_file)
    
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    # keywords = ['vote', 'education']

    for keyword in keywords:

        # starting search
        day = start
        next_day = day + step
        current_step = 0

        count = 0

        print(f'Processing {keyword}...')
        outfile = f'{out_path}/{keyword}.txt'

        # if file exists find last recorded timestamp
        if os.path.exists(outfile):
            with open(outfile, 'r') as fp:
                for line in fp.readlines():
                    data = json.loads(line.strip())

            # get last timestamp
            timestamp_str = data['utc_datetime_str']


            # overwrite current day
            day = datetime.strptime(timestamp_str, dt_format)
            next_day = day + step

        # search each step
        while day <= end:

            # every day
            if current_step % (24 / (step.seconds / 60 / 60)) == 0:
                print(f'    searched through {day} at {datetime.now()}')

            after = int(day.timestamp())
            before = int(next_day.timestamp())
            search = f'{url}/?q={keyword}&after={after}&before={before}&size=100'

            try:
                response = requests.request("GET", search)

            # if we got a response
                if response.status_code == 200:
                    posts = response.json()['data']

                    count += len(posts)

                    # write each post to file
                    for post in posts:
                        with open(outfile, 'a') as fp:
                            fp.write(json.dumps(post) + '\n')


                    # update day
                    day = next_day
                    next_day += step

                    current_step += 1

                    # sleep 2 seconds between queries
                    #time.sleep(2)

                else:
                    print(f'Error message: {response.status_code}. Sleeping 5 seconds')
                    time.sleep(5)
                    print('Trying again...')

            except:
                print('Unknown error. Sleeping 30 seconds.')
                time.sleep(30)
                print('Trying again...')


        print(f'   {count} posts found for {keyword}')
        # sleep 10s before starting a new keyword
        time.sleep(10)

