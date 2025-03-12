import glob
import os
import pandas as pd
import numpy as np

import re
import gensim
from gensim.utils import simple_preprocess
import nltk
#nltk.download('stopwords')
from nltk.corpus import stopwords
stop_words = stopwords.words('english')

import gensim.corpora as corpora
from gensim.test.utils import datapath
from gensim.corpora import MmCorpus


#-------- Global Variables -------- #
platform = 'reddit'
k = 10

# folder to store output conversations as datatables
path = f'../data/{platform}'

# input conversations as data tables
inpath = f'{path}/conversation`_tables'

# file to store full corpus text
corpus_file = f'{path}/all_{platform}_text.txt'

outfile = f'{path}/{platform}_topics.txt'

if not os.path.exists(outpath):
    os.mkdir(outpath)
    

def get_corpus(corpus_file):

    if platform == 'twitter':
        keep = ['tweet_id']
    elif platform == 'reddit':
        keep = ['pid']
        
    # field in both platforms
    keep += ['conversation_id',
            'text',
            'russia',
            'midterms',
            'childcare']

    posts = 0

    for i, filename in enumerate(glob.glob(f'{inpath}/*')):
        cid = filename.split('/')[-1].split('.')[0]
        
        df = pd.read_csv(filename, sep='\t')
        
        # replace nas
        df['conversation_id'].fillna(cid, inplace=True)
        
        # keep only tweets in this conversation
        df = df[df['conversation_id']==cid].reset_index()
    
        df = df[keep]
        
        posts += len(df)
        
        if i == 0:
            df.to_csv(corpus_file, header=True, mode='w', 
                      index=False, sep='\t')
        else:
            df.to_csv(corpus_file, header=False, mode='a',
                     index=False, sep='\t')

    print(f'{posts} posts written to {corpus_file}.')


def sent_to_words(sentences):
    for sentence in sentences:
        # deacc = True removes punctuations
        yield(gensim.utils.simple_preprocess(str(sentence), deacc=True))
        
        
def remove_stopwords(texts):
    return [[word for word in simple_preprocess(str(doc)) 
             if word not in stop_words] for doc in texts]
    
    
def prep_lda(df):
    raw_texts = df.text.values.tolist()
    data_words = list(sent_to_words(raw_texts))

    # remove stop words
    data_words = remove_stopwords(data_words)
    
    # Create Dictionary
    id2word = corpora.Dictionary(data_words)

    # Create Corpus
    texts = data_words

    # Term Document Frequency
    corpus = [id2word.doc2bow(text) for text in texts]
    
    return id2word, corpus

    
if __name__ == "__main__":
    
    if not os.path.exists(corpus_file):
        print('Creating corpus...')
        get_corpus(corpus_file)
    
    # load corpus
    print('Loading corpus...')
    df = pd.read_csv(corpus_file, sep='\t')
    print(df.columns)
    pid_field = df.columns[0]
    
    # lower case all text
    df['text'] = df['text'].str.lower()
    
    print(f'Prepping for LDA')
    id2word, corpus = prep_lda(df)
    
    print('Running topic model')
    lda_model = gensim.models.LdaMulticore(corpus=corpus,
                                       id2word=id2word,
                                       num_topics=k,
                                       random_state=42)

    print('Topic model complete, saving to file')
    
    # save model 
    lda_model.save(f'{outpath}/{platform}_lda.model')

    # save corpus
    MmCorpus.serialize(f'{outpath}/{platform}_corpus.mm', corpus)
    
    # get document topics
    document_topics = lda_model.get_document_topics(corpus)
    
    print('Saving topic loadings...')
    # create table of pid to topic loadings
    posts = df[pid_field].values.tolist()

    topic_dists = np.zeros((len(posts), k))

    for i, post in enumerate(posts):
        topic_dist = document_topics[i]

        for j, val in topic_dist:
            topic_dists[i][j] = val

    # save as pandas dataframe
    topic_df = pd.DataFrame(topic_dists, index=posts)
    
    topic_df = topic_df.reset_index()
    topic_df.rename(columns={'index':pid_field}, inplace=True)

    # save topics to file
    topic_df.to_csv(outfile, sep='\t', index=False)
    
    print(f'{platform} LDA calculated and written to file')