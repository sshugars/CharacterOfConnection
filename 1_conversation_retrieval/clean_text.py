import json
import re
from spacy.tokenizer import Tokenizer
from spacy.lang.en import English
from spacy.matcher import PhraseMatcher

#-------- Global Variables -------- #
nlp = English()
tokenizer = nlp.tokenizer

# keyword file
keyword_file = 'keyword_dict.json'

def get_terms(keywords, verbose=True):
    term_to_topic = dict()

    exclude = ['is', 'no', 'with', 'ber',
              'stop', 'now', 'state', 'stand', 'support', 'arm', 'warin',
              'save', 'blue', 'reduction', 'act', 'vember']

    for topic, word_list in keywords.items():
        for word in word_list:
            
            # add direct keyword
            term_to_topic[word.lower()] = topic
            
            # look for substrings
            words = re.findall('[A-Z][^A-Z]*', word)
            
            if words:
                for sub in words:
                    sub = sub.lower()
                    
                    if sub not in exclude:
                        term_to_topic[sub] = topic

    if verbose:
        print(f'{len(term_to_topic)} unique keywords found.')

    return term_to_topic


def load_keywords(verbose=True):
    with open(keyword_file, 'r') as fp:
        keywords = json.loads(fp.read())
        
    term_to_topic = get_terms(keywords, verbose)

    return term_to_topic


def get_clean_text(text):
    words = text.split()

    clean_words = [word.strip() for word in words]

    clean_text = ' '.join(clean_words)

    return clean_text


def match_text(text, term_to_topic):
    text = text.lower()

    terms = set()
    lists = set()

    for term, topic in term_to_topic.items():
        if term in text:
            terms.add(term)
            lists.add(topic)

    return terms, lists