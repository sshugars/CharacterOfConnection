import pandas as pd
import numpy as np
import spacy
import os
import glob
from datetime import datetime

from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report


corpus_file = f'../../data/full_corpus.txt'
labeled_corpus_file = f'../../data/labeled_corpus.txt'
chunk_folder = f'../../data/classified'

model_path = 'model'

topics = {'Childcare/parenting':'child', 
          'Russia/Ukraine war':'russia', 
          'US midterm elections':'midterms'}

thresh = .8


def split_corpus(nchunks=50):
    
    if not os.path.exists(chunk_folder):
        os.mkdir(chunk_folder)
    
    # full corpus
    corpus = pd.read_csv(corpus_file, sep='\t',
                         dtype={'pid':str, 'conversation_id':str})
    print(f'Full corpus loaded ({len(corpus)} posts).')
    
    # split into chunks
    chunks = np.array_split(corpus, nchunks)
    
    for i, chunk in enumerate(chunks):
        chunk.to_csv(f'{chunk_folder}/chunk_{i}.txt', sep='\t', index=0)
    
    print(f'{i+1} chunks written to {chunk_folder}')

def get_test_train(df, topic):
    '''
    Recovers test/train split used in model development
    Allows for testing/reports beyond what's built-in to Spacy
    '''
    
    df['code'] = np.where(df['final_topic']==topic, 'Yes', 'No')

    # balance classes
    yes_df = df[df['code']=='Yes']
    no_df = df[df['code']=='No'].sample(n=len(yes_df), random_state=42)
    balanced_df = pd.concat([yes_df, no_df])

    # split data: 80% train, 20% test
    train, test = np.split(balanced_df.sample(frac=1, random_state=42), 
                                     [int(.8*len(balanced_df))])
    
    return train, test


def predict_class(df, topic):
    # Predict topical class
    on_topic_score = df['clean_text'].apply(lambda x: nlp(x).cats[topic])
    df[topic]  = np.where(on_topic_score > thresh, 1, 0)
    
    return df


def get_report(df, topic):
    # recover original test/train split
    train, test = get_test_train(df, topic)
    
    # predict class
    test = predict_class(test, topic)

    # re-label for reporting ease
    test['pred'] = np.where(test[topic] == 1, 'on_topic', 'off_topic')
    test['true'] = np.where(test['code']=='Yes', 'on_topic', 'off_topic')

    # get classification report
    report = classification_report(test['true'], test['pred'])
    
    # write to file
    outfile = f'../../data/{topics[topic]}_classification_scores.txt'
    
    with open(outfile, 'w') as fp:
        fp.write(report)

    # print classification report
    print(report)
    
if __name__ == "__main__":
    # training data
    #df = pd.read_csv(labeled_corpus_file, sep='\t', 
    #                 dtype={'pid':str, 'final_topic':str})
    
    #print('Labled data loaded')
    #split_corpus(nchunks=50)
          
    # get confusion matrix
    #for topic in topics:
    #    print(f'Processing {topic}...')
    #    model = f'{model_path}/{topics[topic]}_filtered/model-best'
    #    nlp = spacy.load(model)

    #    get_report(df, topic)
        
    # classify each chunck
    for i, filename in enumerate(glob.glob(f'{chunk_folder}/*')):
        print(f'Processing chunk {i}: {filename} at {datetime.now()}')
        corpus = pd.read_csv(filename, sep='\t',
                             dtype={'pid':str, 'final_topic':str})
        
        corpus = corpus.dropna(subset='clean_text')
                              
        for topic in topics:
            if topic not in corpus.columns:
                print(f'  Predicting labels for {topic}...')
                model = f'{model_path}/{topics[topic]}_filtered/model-best'
                nlp = spacy.load(model)
                corpus = predict_class(corpus, topic)
            else:
                print(f'  {topic} already estimated for {filename}')
        
        # writing updated corpus to file
        print(f'  Writing updated chunk {i} to {filename}')
        corpus.to_csv(filename, sep='\t', index=0)