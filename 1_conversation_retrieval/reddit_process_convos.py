import numpy as np
import json
import gzip
import glob
import os
import shutil
from datetime import datetime
import clean_text as ct

#-------- Global Variables -------- #
path = '../data'
reddit_path = f'{path}/Reddit'
in_convo_path = f'{reddit_path}/raw/threads'
out_convo_path = f'{reddit_path}/conversation_tables'

topics = ['russia',
          'midterms',
          'childcare']

str_fmt = '%Y-%m-%dT%H:%M:%S.000Z'

def start_file(outfile):
    
    # header for all files
    header = ['pid',
              'conversation_id',
              'author',
              'parent_id',
              'permalink', 
              'over18',
              'text',
              'created_at',
              'topics',
              'matched_words'] + topics

    # write on creation
    with open(outfile, 'w') as fp:
        fp.write('\t'.join(header) + '\n')
        
    return header


def write_to_file(data_dict, header, outfile):

    line = '\t'.join([str(data_dict[key]) for key in header])

    with open(outfile, 'a') as fp:
        fp.write(line + '\n')
        
        
def handle_orphans(reply_data, header, outfile):
    orphans = reply_data['children']

    for orphan in orphans:
        data_dict = dict((key, np.nan) for key in header)

        # overwrite
        data_dict['pid'] = orphan
        data_dict['parent_id'] = reply_data['parent_id'].split('_')[-1]
        data_dict['conversation_id'] = conversation_id
        
        write_to_file(data_dict, header, outfile)
        
        
def process_replies(replies, header, conversation_id):
    for reply in replies:
        reply_data = reply['data']
        
        try:
            data_dict = get_details(reply_data)
            data_dict['conversation_id'] = conversation_id
            write_to_file(data_dict, header, outfile)
        
        # handle condensed data
        except KeyError:
            handle_orphans(reply_data, header, outfile)

        # check if more replies
        try:      
            if reply_data['replies'] != '':
                children = reply_data['replies']['data']['children']
                process_replies(children, header, conversation_id)
        except:
            pass
        
        
def get_details(post):
    
    #### Post ID
    pid = post['id'] # will be thread id for root
    
    #### Author
    author = post['author']
    
    ### Created at
    created_at = datetime.fromtimestamp(post['created_utc']).strftime(str_fmt)
    
    
    # look for parent
    try:
        parent_id = post['parent_id'].split('_')[-1]
    except KeyError:
        parent_id = np.nan
    
    #### permalink url
    try:
        permalink = post['permalink']
    except KeyError:
        permalink = ''
    
    #### flag explicit content
    try:
        over18 = post['over_18']
    except KeyError:
        over18 = False
    
    ##### Text
    try: 
        title = post['title']
    except KeyError:
        title = ''
        
    try:
        body = post['selftext']
    except KeyError:
        try:
            body = post['body']
        except KeyError:
            body = ''
            
    text = title + ' ' + body
    text = ct.get_clean_text(text)

    # match tokens
    matches, lists_matched = ct.match_text(text, term_to_topic)
    
        
    data_dict = {'pid' : pid,
                 'author': author,
                 'parent_id': parent_id,
                 'permalink': permalink,
                 'text': text,
                 'created_at': created_at,
                 'over18': over18,
                 'topics': ' | '.join(lists_matched),
                 'matched_words': ' | '.join(matches)
              }
    
    for topic in topics:
        if topic in lists_matched:
            data_dict[topic] = 1
        else:
            data_dict[topic] = 0
    
    return data_dict


if __name__ == "__main__":

    print('Loading keywords...')
    term_to_topic = ct.load_keywords()

    # create out folder
    if not os.path.exists(out_convo_path):
        os.mkdir(out_convo_path)

    print('Processing conversations...')
    new = 0
#    skipped = 0

    # process conversations
    for i, convo_file in enumerate(glob.glob(f'{in_convo_path}/*')):
        
        if i%1000==0:
            print(f'...Processed file {i} files at {datetime.now()}')
        
        convo_id = convo_file.split('/')[-1].split('.')[0].split('_')[-1]
        outfile = f'{out_convo_path}/{convo_id}.txt'
        
        # if we haven't already processed this conversation
        if not os.path.exists(outfile):
            # load data
            with open(convo_file, 'r') as fp:
                data = json.loads(fp.read())

            header = start_file(outfile)

            # process root
            #try:
            root = data[0]['data']['children'][0]['data']
            data_dict = get_details(root)

            # conversation_id is root id
            conversation_id = data_dict['pid']
            data_dict['conversation_id'] = conversation_id
            write_to_file(data_dict, header, outfile)

            # process children
            children = data[1]['data']['children']

            for child in children:
                try:
                    data_dict = get_details(child['data'])
                    data_dict['conversation_id'] = conversation_id
                    write_to_file(data_dict, header, outfile)

                except KeyError:
                    handle_orphans(child['data'], header, outfile)

                # replies
                if 'replies' in child['data']:
                    if child['data']['replies'] != '':
                        replies = child['data']['replies']['data']['children']
                        process_replies(replies, header, conversation_id)
                else:
                    pass
            #except:
            #    print(f'Skipping file {convo_file}')
            #    skipped += 1


    print(f'{i} total conversations found.')
    print(f'{new} new conversations processed.')
#    print(f'{skipped} conversations skipped.')