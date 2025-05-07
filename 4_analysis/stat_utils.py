import json
import re
from spacy.tokenizer import Tokenizer
from spacy.lang.en import English
from spacy.matcher import PhraseMatcher



# keyword file
keyword_file = '../1_conversation_retrieval/keyword_dict.json'

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

    # add missing terms
    for item in ['2022elections', 'elections', 'midtermelections2022', 'midterms2022',
             'nodember', 'roevember', 'savedemocracy', 'vote']:
        term_to_topic[item] = 'midterms'

    for item in ['armukrainenow','russianukrainewar','russiaukraineconflict','standwithukraine',
             'stoprussia','stoprussianaggression','stoprussianow','supportukraine',
             'ukrainerussia','ukrainerussiaconflict','ukrainerussianwar','ukrainerussiawar',
             'ukrainewar','warinukraine', 'warukraine']:
        term_to_topic[item] = 'russia'

    for item in ['childcare','childhealth','children','daycare','earlychildhoodeducation',
             'education','healthychildren','kids','kidshealth','nursery','preschool']:
        term_to_topic[item] = 'childcare'

    if verbose:
        print(f'{len(term_to_topic)} unique keywords found.')

    return term_to_topic


def load_keywords(verbose=False):
    with open(keyword_file, 'r') as fp:
        keywords = json.loads(fp.read())
        
    term_to_topic = get_terms(keywords, verbose)

    return term_to_topic