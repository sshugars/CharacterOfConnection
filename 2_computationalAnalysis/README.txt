This folder contains code for computational analysis of raw data, specifically estimating a topic model and calculating toxity scores. 

# Toxicity

## twitter_calc_tox.py
Uses Google Perspectives API to calculate toxicity score for every tweet. Creates new 'tox' column indicating returned score

Input: `../data/Twitter/conversation_tables` folder (output from `twitter_clean_conversation_tables.py`)
Outputs: 
* Overwrites files in `conversation_tables`
* `../reference/tox_calculated.txt`, file to track ids for which toxicity has been calculated


Notes:
* Saves processed conversation_ids to `/reference/tox_calculated.txt`, ignores processed files on restart
* Will check for any already calculated tox scores and only calculate scores for missing records
* Tweets with insufficient text will return a score of np.nan, so having a score missing doesn't necessarily mean that score wasn't calculated
* This code is currently set up for a second passing -- checking that all scores which could be calculated were calculated


## reddit_calc_tox.py
Uses Google Perspectives API to calculate toxicity score for every tweet. Creates new 'tox' column indicating returned score

Input: `{reddit_path}/conversation_tables`, output from `reddit_process_convos.py`
Output: `{reddit_path}/conversation_tables` (overwrites original files)


# Topic Model

## topic_model.py
Run a topic model on a given platform's content

Inputs:
* "platform": str, either 'reddit' or 'Twitter'
* `../data/{platform}/conversation_tables`, data tables for input platform

Outputs:
* `all_{platform}_text.txt`: corpus for given platform
* `'{path}/{platform}_topics.txt` dataframe of topic distribution for all conversations