"""
Sam Maurer - Oct 2018 - python 3.6

Load tweets from the westcoast monthly samples into HDF5 files, keeping only certain data 
fields.

"""
import json
import os
import re
import time
import zipfile

import numpy as np
import pandas as pd

from datetime import datetime as dt
from dateutil import tz


IN = '/Users/maurer/Dropbox/Data/Twitter/Westcoast-monthly-samples/json/'
OUT = '/Users/maurer/Dropbox/Data/Twitter/Westcoast-monthly-samples/hdf/'

FILE_PREFIX = 'westcoast-'

def main():

#    dirs = []
    dirs = ['']
    
    for dir in dirs:
        
        # Get list of dates covered by raw files
        dates = []
        for f in os.listdir(IN + dir):
            try:
                date = f.split('-')[1]
                dates.append(date)
            except:
                continue
        dates = np.unique(dates)
    
        # Generate an H5 file for each of the dates
        for date in dates:
            process_files(base_dir = IN + dir, 
                          out_path = OUT + dir + FILE_PREFIX + date + '.h5',
                          min_date = int(date),
                          max_date = int(date) + 1)

    # Process individual dates if needed to recover from errors
    dates = []
#    dates = ['20180702', '20180801', '20180901']
    
    for date in dates:
        dir = ''
        process_files(base_dir = IN + dir, 
                      out_path = OUT + dir + FILE_PREFIX + date + '.h5',
                      min_date = int(date),
                      max_date = int(date) + 1)


# Helper functions

def get_filepaths(base_dir, min_date, max_date):
    """
    Assemble list of paths for input files, based on date range.
    
    Filenames have format 'prefix-date-time.json.zip'.
    
    """
    flist = []
    for f in os.listdir(base_dir):
        try:
            date = f.split('-')[1]
            if int(date) >= int(min_date) and int(date) < int(max_date):
                flist.append(base_dir + f)
        except:
            continue
    return flist


def trim_zip(fpath):
    return fpath.split('/')[-1].split('.zip')[0]


def parse_source(source_str):
    """
    Extract the name of the Twitter client from 'source' string.
    
    """
    if source_str == 'web':
        return source_str
    try:
        return re.split('<|>', source_str)[2]
        # return string.split(string.split(source_str,'>')[1],'<')[0]
    except:
        return ''


def parse_tweet(line):
    """
    Extract the relevant fields for each tweet.
    
    """
    t = json.loads(line)
    fields = {
        'id': t['id'],
        'user_id': t['user']['id'],
        'user_name': t['user']['screen_name'],
        'text': t['text'],
        'lon': None,
        'lat': None,
        'place_id': None,
        'place_name': None,
        'place_country': None,
        'place_type': None,
        'bb_coords': None,
        'ts': t['created_at'],
        'utc_offset': t['user']['utc_offset'],
        'lang': t['lang'],
        'source': parse_source(t['source']),
        'user_loc': t['user']['location']
    }
    if t.get('coordinates'):
        fields['lon'] = t['coordinates']['coordinates'][0]
        fields['lat'] = t['coordinates']['coordinates'][1]
    if t.get('place'):
        fields['place_id'] = t['place']['id']
        fields['place_name'] = t['place']['full_name']
        fields['place_country'] = t['place']['country_code']
        fields['place_type'] = t['place']['place_type']
        fields['bb_coords'] = t['place']['bounding_box']['coordinates']
    return fields


def parse_limit_message(line):
    """
    Extract the relevant content from a limit message.
    
    """
    t = json.loads(line)
    return {
        'limit_track': t['limit']['track'],
        'timestamp_ms': t['limit']['timestamp_ms']
    }


def process_files(base_dir, out_path, min_date, max_date):
    """
    out_path - absolute or relative to where the script is run from
    
    Possible streaming messages: http://geduldig.github.io/TwitterAPI/errors.html
    
    """
    t0 = time.time()
    paths = get_filepaths(base_dir, min_date, max_date)

    tweets = []
    messages = []
    errors = []

    for p in paths:
        with zipfile.ZipFile(p) as z:
            with z.open(trim_zip(p)) as f:
                for line in f:
                    # Note that `line` is a byte object, at least in python 3
                    try:
                        if b'"text":' in line:
                            tweets.append(parse_tweet(line))
                        elif b'"limit":' in line:
                            messages.append(parse_limit_message(line))
                    
                    except (KeyError, TypeError):
                        # A key is missing, or a top-level key is present but doesn't 
                        # contain the expected data type -- log it.
                        errors.append(line.decode('utf-8'))
                        
                    except Exception as e:
                        raise
#                         print(str(dt.now()) + "\n{}: {}".format(type(e).__name__, e))
#                         continue

    df = pd.DataFrame(tweets)

    print(len(df))

    # "Fixed" format is faster for reading and writing, while "table" format allows 
    # searching and subsetting of the data when we subsequently load it from the Store. 
    # This would be useful, but it also disallows unicode data, so skipping it for now. 
    # See notebook for more details. 

    df.to_hdf(out_path, 'tweets', mode='w', format='fixed')
    
    mdf = pd.DataFrame(messages)
    mdf.to_csv(out_path + '.messages.csv', index=False)
    
    with open(out_path + '.errors.txt', 'w') as outf:
        outf.writelines(errors)
    
    print(str(int(time.time()-t0)//60) + ' min.')


main()
