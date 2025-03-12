This folder contains code for preprocessing raw data. Relative to this file, this data is stored in `../data`

*************************
clean_text.py
*************************
Has helper functions to:
* load keywords
* ensure consistent parsing of text and identification of keywords

often loaded into other files as
import clean_text as ct


# Twitter
Seed tweets were collected using the scripts available in the `0_seed_posts` subfolder. This document goes through the steps of pre-processing this data and collecting and pre-processing full conversations.

Note that pre-processing of seed tweets informed collection of conversations. Specifically, conversation collection was prioritized to ensure conversations from all topics were able to be collected.

### Pre-processing steps and process
* `twitter_process_stream.py` goes through all collected seed tweets and stores a record of tweet_id, conversation_id, and topic(s) files are stored by order seen in `../../data/Twitter/raw/seed_summaries`
* `twitter_get_seed_location.py` goes through all collected seed tweets and creates a record of the file location for each seed tweet. Files are stored by conversation_id in `../../data/Twitter/raw/seed_locations`
* `twitter_sample_convos.ipynb` samples conversations based on seed tweet topics and examines distribution of collected tweets
* `data_collection/twitter_get_convos.py` can then be used to collect full conversations
* `twitter_process_convos.py` goes through json from collected convos and converts each conversation to a single data table. Data is stored by conversation_id in `../../data/Twitter/conversation_tables`
* `twitter_clean_conversation_tables.py` drops duplicates from table and searches for additional tweets to include based on seed data. Data overwrites original file.
* `twitter_calc_tox.py` iterates over all conversation tables and runs each tweets' text through the Perspectives API.


## twitter_sample_convos.ipynb
Creates several lookup tables and samples conversations to search

Input: `../../data/Twitter/raw/seed_summaries`
	`../../data/Twitter/raw/conversation_errors`
Output:
* `../../reference/twitter_seed_topic_convo.json` : dict of {topic : [list of conversation_ids]}
* `../../reference/twitter_seed_convo_topic.json` : dict of {conversation_id : topic}
* `../../reference/no_convos.txt` : list of conversation_ids that were searched but had no tweets attached
* `../../reference/convos_not_searched.json` : dict of {topic: [list of conversation_ids not searched]}
* `../../reference/twitter_convos_to_search.txt` : list of conversation_ids to search next

The conversation_ids in `twitter_convos_to_search.txt` can then be used as input to `data_collect/twitter_get_convos.py`

This file contains code to:

* Determine topical distribution of seed tweets/conversations
* Examine searched conversations by topic
* Sample conversations for collection by topic

## twitter_get_convos.py

Script for collecting full Twitter conversations 

Inputs:
* `convo_ids.txt`: file of convo ids from seed tweets (output from `tweet_seed_collection.py`

Outputs:
* `/raw/conversations`: folder of retrieved conversations as .json.gzip files

## twitter_process_convos.py

Input: `../../data/Twitter/raw/conversations` folder
Output: `../../data/Twitter/conversation_tables` folder

Iterates over all raw conversation json files (collected by `data_collect/twitter_get_convos.py`) and creates data tables of key fields


Notes:
* Uses `clean_text.py` to determine topical keyword matches for every retrieved tweet.
* Appends all seed tweets associated with a conversation_id. This MAY result in duplicated tweets (later cleaned in `twitter_clean_conversation_tables.py`)
* Searches BOTH `['data']` and `['includes']['tweets']` to retrieve as much data as possible


## twitter_clean_conversation_tables.py

Input: `../data/Twitter/conversation_tables`
Output: `../data/Twitter/conversation_tables` (overwrites original files)

Clean conversation tables and ensure all data included

Notes:
* Uses `seed_locations/{conversation_id}_seed.txt` to check for tweets that are part of this conversation that were not captured by the conversation search
* Uses `clean_text.py` to determine keyword matches any newly included tweets (ie, tweets found in seed files)
* Drops any duplicate rows (which may occur if a tweet was collected multiple times)
* Overwrites old conversation_table with clean conversation table data


## twitter_calc_tox.py

Input: `../data/Twitter/conversation_tables`
Output: `../data/Twitter/conversation_tables` (overwrites original files)

Uses Google Perspectives API to calculate toxicity score for every tweet
Creates new 'tox' column indicating returned score

Notes:
* Saves processed conversation_ids to `/reference/tox_calculated.txt`, ignores processed files on restart
* Will check for any already calculated tox scores and only calculate scores for missing records
* Tweets with insufficient text will return a score of np.nan, so having a score missing doesn't necessarily mean that score wasn't calculated
* This code is currently set up for a second passing -- checking that all scores which could be calculated were calculated


# Reddit

## reddit_get_convos.py
Script for getting entire conversations based on seed comments

Input: `{reddit_path}/raw/comment_search' (Folder of output from `reddit_seed_collection.py`)

Outputs:
* `{reddit_path}/threads` : folder with all retrieved threads
* `{reddit_path}/unretrievable_threads.txt` : record of irretrievable threads

## reddit_process_convos.py
Script to turn collected convos into cleaned data tables

Input: `{reddit_path}/threads`, folder with all retrieved threads (output from `reddit_get_convos.py`
Output: `{reddit_path}/conversation_tables`, folder with key data stored in tables


## reddit_calc_tox.py
Calculate toxity of each comment

Input: `{reddit_path}/conversation_tables`, output from `reddit_process_convos.py`
Output: `{reddit_path}/conversation_tables` (overwrites original files)

