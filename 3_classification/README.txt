Code in this folder uses the handcoded results to classify all posts in the corpus. Note: handcoded data is stored within the project file.

# Handcoding

## handcoded.ipynb
Consolidates all hand-coded data, calculates coder agreement, and assigns final handcoded labels. Output is used to train the classifier.

Input: `../../data/handcoded` folder
Output: `../../data/handcoded/handcoded.txt` file

# Classification

## text_prep.py
Completes data pre-processing and splits into training and test sets

Inputs:
* `all_reddit_text.txt`
* `all_twitter_text.txt`

Outputs:
*  `full_corpus.txt` : full corpus for given platform
*  `labeled_corpus.txt` : hand labeled corpus for given platform
*  `spacy_models` folder with split binaries

## classify_posts.py
Run the classification. Script runs a multiclassifiation + binary classification for each topic.

Inputs:
*  `full_corpus.txt` : full corpus for given platform
*  `labeled_corpus.txt` : hand labeled corpus for given platform

Outputs:
*  `classified` folder with classifications

## assign_convo_labels.py
Determines final conversation-level classifcations.

Inputs:
* `classified` folder

Outputs:
* `conversation_to_topic.txt': conversations with final classification
* `no_topic.txt` : conversation with no topic classification