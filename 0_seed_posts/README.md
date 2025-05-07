This folder contains code for collecting and cleaning seed posts from Twitter and Reddit, using the APIs that were available at the time.

All data is saved into a `data` file that is separate from the code file with a relative path of `../data`.

Specific files in this folder are:

# Twitter data

## keywords.txt
Supplementary file with keywords used to search Twitter


## tweet_seed_collection.py
Script for collecting tweets with keyword match

Input: `keywords.txt`
Output: `raw/stream`, folder of all retrieved seed tweets as .json.gzip files


# twitter_process_stream.py
Input: `../data/Twitter/raw/stream` folder (output from `tweet_seed_collection.py`)
Output: `../data/Twitter/raw/seed_summaries` folder

This script creates a record of all seed topics, tweet_ids, and conversation_ids

Iterates through all seed tweets and saves record of tweet_id, conversation_id, and topic(s)

Notes: 
* ONLY records tweet if at least 1 topic is matched
* ONLY checks retrieved tweets, not includes
* Saves data to multiple files stored in 
	f'{twitter_path}/raw/seed_summaries'
* Each file contains tweets from 500 searches (approx 50k tweets)
* Tweet records stored in order received
* Summary retains `tweet_id`, `conversation_id`, and `topic`


## twitter_get_seed_location.py
Input: `../data/Twitter/raw/stream` folder
Output:  `../data/Twitter/raw/seed_locations` folder

This script creates lookup tables for each seed conversation
indicating storage location of seed tweets tied to that conversation

Iterates through all seed tweets and saves
for each conversation_id:
tweet_id, filename, and whether or not tweet is in an include

Notes: 
* Each coversation_id its own file {conversation_id}_seeds.txt
* 'includes' is boolean. 1 means saved in `['includes']['tweets']` of `filename` data. This indicates a retweet or other "included" item



# Reddit data

## keywords_reddit.txt
Supplementary file with keywords used to search Reddit

## reddit_seed_collection.py
Script for identifying Reddit comments with keyword match.

Inputs: `keywords_reddit.txt`

Output: `{reddit_path}/raw/comment_search' folder with .json dumps of search




