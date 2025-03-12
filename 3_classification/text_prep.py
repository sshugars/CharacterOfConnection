import pandas as pd
import numpy as np
import glob
import os
import json

from tqdm import tqdm
import string
import re
import spacy
from spacy.tokens import DocBin
from spacy.lang.pt.stop_words import STOP_WORDS

###### Global Variables #######
nlp = spacy.load("en_core_web_trf")

path = '../../data/'
labeled_data_file = f'{path}/handcoded/handcoded.txt'

spacy_path = f'spacy_models'
if not os.path.exists(spacy_path):
    os.mkdir(spacy_path)

# input data: stored outside project file
reddit_corpus_file = f'../../data/reddit/all_reddit_text.txt'
twitter_corpus_file = f'../../data/twitter/all_twitter_text.txt'

# output data: stored within project file
corpus_file = f'../../data/full_corpus.txt'
labeled_corpus_file = f'../../data/labeled_corpus.txt'

topics = {'Childcare/parenting':'child', 
          'Russia/Ukraine war':'russia', 
          'US midterm elections':'midterms'}


def normalize_text(text):

    lim = 512
    
    # lower case all text
    text = text.lower()

    # remove @ mentions
    text = re.sub(r'@\w+', '', text).strip()

    # remove urls
    text = re.sub(r'https?://[A-Za-z0-9./]+', ' ', text)
    text = re.sub(' +', ' ', text)
    
    # remove stop words and clean indvidual tokens
    tokens = text.split()
  
    tokens = [t.strip() for t in tokens if 
              t not in STOP_WORDS and 
              t not in string.punctuation and 
              len(t) > 3]
    
    # ensure text isn't longer than limit
    text = tokens[:lim]
    
    if len(text) >= lim-200:
        tokens = [token.text for token in nlp(' '.join(text))]
        tokens = [str(t.strip()) for t in tokens if 
                  t not in STOP_WORDS and 
                  t not in string.punctuation and 
                  len(t) > 3]
        
        text = tokens[:lim]


    return ' '.join(text)


def load_data():
    if os.path.exists(labeled_corpus_file):
        df = pd.read_csv(labeled_corpus_file, sep='\t',
                         dtype={'pid':str, 'final_topic':str})
        
        print(f'{len(df)} total comments coded')
    
    else:

        # load handcoded data
        coded = pd.read_csv(labeled_data_file, sep='\t')
        print(f'{len(coded)} conversations coded')


        # load reddit corpus
        reddit_corpus =  pd.read_csv(reddit_corpus_file, sep='\t',
                                     dtype={'pid': str,
                                            'conversation_id':str})

        print(f'{len(reddit_corpus)} Reddit posts loaded.')

        # load twitter corpus
        twitter_corpus =  pd.read_csv(twitter_corpus_file, sep='\t',
                                      dtype={'tweet_id': str,
                                            'conversation_id':str})

        print(f'{len(twitter_corpus)} Twitter posts loaded.')

        # concatinate reddit and twitter corpus for one corpus
        twitter_corpus.rename(columns={'tweet_id':'pid'}, inplace=True)

        reddit_corpus['platform'] = 'reddit'
        twitter_corpus['platform'] = 'twitter'

        corpus = pd.concat([reddit_corpus, twitter_corpus])
        
        print('Cleaning text...')
        # clean text
        corpus = corpus.dropna(subset='text')
        corpus['clean_text'] = corpus['text'].apply(lambda x: normalize_text(x))
        corpus = corpus[corpus['clean_text']!='']

        print(f'{len(corpus)} total posts loaded.')
        corpus.to_csv(corpus_file, sep='\t', index=False)
        print('Full corpus written to file')
        
        ####### remove next two lines when you uncomment above
        #corpus = pd.read_csv(corpus_file, sep='\t',
        #                     dtype={'pid':str, 'conversation_id':str})
        #print('Full corpus loaded.')

        # coded comments
        df = corpus.merge(coded, on='conversation_id', how='inner')

        # make all off-topic coded as 'None'
        df['final_topic'] = df['final_topic'].replace('Not sure', 'Off_topic')
        df['final_topic'] = df['final_topic'].replace('discuss', 'Off_topic')
        df['final_topic'] = df['final_topic'].fillna('Off_topic')
        
        # if this post has no keyword matches, mark as off topic
        df['key'] = np.where(np.sum(df[['russia', 'midterms', 'childcare']], axis=1)!=0, 1, 0)
        df.loc[df['key'] == 0, 'final_topic'] = 'Off_topic'

        print(f'{len(df)} total comments coded')
        df = df[['pid', 'final_topic', 'clean_text']]
        

        df.to_csv(labeled_corpus_file, sep='\t', index=False)

    return df


def preprocess_multiclass(df):
    # Store the data into tuples
    data = tuple(zip(df.clean_text.tolist(), df.final_topic.tolist())) 

    # Storage for docs
    docs = list()

    # One-hot encoding for the classifications
    for doc, label in tqdm(nlp.pipe(data, as_tuples=True), total = len(data)):
        doc.cats['none'] = label == 'Off_topic'
        doc.cats['children'] = label == 'Childcare/parenting'
        doc.cats['russia'] = label == 'Russia/Ukraine war'
        doc.cats['midterm'] = label == 'US midterm elections'
        docs.append(doc)
        
    return docs


def preprocess_binary(df, topic):
    # Store the data into tuples
    data = tuple(zip(df.clean_text.tolist(), df.code.tolist())) 

    # Storage for docs
    docs = list()

    # One-hot encoding for the classifications
    for doc, label in tqdm(nlp.pipe(data, as_tuples=True), total = len(data)):
        doc.cats[topic] = label == 'Yes'
        doc.cats[f'not_{topic}'] = label == 'No'
        docs.append(doc)
        
    return docs


def get_binary(sub, name, class_type):
    print(f'Creating {name} data...')
    if class_type == 'multiclass':
        docs = preprocess_multiclass(sub)
    elif class_type == 'binary':
        short, topic = name.split('_')
        docs = preprocess_binary(sub, topic)
        name = f'{short}_{topics[topic]}_filtered'
        
    doc_bin = DocBin(docs=docs)
    doc_bin.to_disk(f'{spacy_path}/{name}_{class_type}.spacy')


if __name__ == "__main__":
    
    df = load_data()
    

    
    for topic in topics:
        print(f'Processing data for {topic}...')
        df['code'] = np.where(df['final_topic']==topic, 'Yes', 'No')
        
        # balance classes
        yes_df = df[df['code']=='Yes']
        no_df = df[df['code']=='No'].sample(n=len(yes_df), random_state=42)
        balanced_df = pd.concat([yes_df, no_df])

        # split data: 75% train, 15% validate, 10% test
        # train, validate, test = np.split(balanced_df.sample(frac=1, random_state=42), 
        #                                 [int(.75*len(balanced_df)), int(.9*len(balanced_df))])
        
        # split data: 80% train, 20% test
        train, test = np.split(balanced_df.sample(frac=1, random_state=42), 
                                         [int(.8*len(balanced_df))])


        get_binary(train, f'train_{topic}', 'binary')
        get_binary(validate, 'validate', f'{topics[topic]}', 'binary')
        get_binary(test, f'test_{topic}', 'binary')

    
    print('Data saved to file--ready to train model!')