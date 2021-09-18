import requests
from bs4 import BeautifulSoup
import pandas as pd
import tweepy
import json 
import time
import os

mp_url = 'https://www.althingi.is/thingmenn/althingismenn/'


auth = tweepy.OAuthHandler(os.environ.get('consumer_key', ''), os.environ.get('consumer_secret', ''))
auth.set_access_token(os.environ.get('access_token', ''), os.environ.get('access_token_secret', ''))
api = tweepy.API(auth, wait_on_rate_limit=True)

# Fetch the list of MPs from althingi.is along with basic attributes
def get_mps():
    try:
        df = pd.read_csv('./mps.csv')
    except: 
        mps = []
        soup = BeautifulSoup(requests.get(mp_url).content, 'html.parser')
        table = soup.find(id='t_thingmenn')
        rows = [c for c in table.findChildren('tr')]
        for index, row in enumerate(rows):
            if index == 0:
                # headers
                continue
            # nafn, skammstofun, kjordaemanumer, kjordaemi, flokkur
            data = [td.text for td in row.findAll('td')]
            link = row.find('a')
            titles = row.find('th').findAll('span')
            if len(titles) > 1:
                titles = [node.text for node in titles[1:]]
            else: 
                titles = []
            mps.append({
                'link': link.get('href'),
                'name': link.text,
                'abbreviation': data[0],
                'number': data[1],
                'precinct': data[2],
                'party': data[3],
                'titles': titles,
            })
        df = pd.DataFrame.from_records(mps)
        df.to_csv('./mps.csv')
    return df


# Fetch a twitter link, if one exists, from the mp detail page 
def get_twitter_link(link):
    detail_link = 'https://www.althingi.is/{0}'.format(link)
    soup = BeautifulSoup(requests.get(detail_link).content, 'html.parser')
    if twitter_link := soup.find('a', class_='twitter'):
        return twitter_link.get('href')
    else:
        return None


def get_twitter_details(twitter_link):
    twitter_handle = twitter_link.split('/')[-1].replace('@', '')
    try: 
        user = api.get_user(twitter_handle)
        with open ('./data/{0}.json'.format(twitter_handle), 'w') as fp:
            json.dump(user._json, fp)
        user_data = {
            'username': twitter_handle,
            'twitter_link': twitter_link,
            'name': user.name,
            'created_at': user.created_at,
            'id': user.id,
            'followers_count': user.followers_count,
            'favourites_count': user.favourites_count,
            'friends_count': 914,
            'profile_image_url': user.profile_image_url,
            'url': user.url,
            'verified': user.verified,
            'description': user.description,
            'statuses_count': user.statuses_count,
            'last_tweet': user.status.created_at,
        }
        return user_data
    except Exception as e:
        return {}


def get_twitter_info(mps):
    try:
        return pd.read_csv('./twitter-info.csv')
    except: 
        twitter_info = []
        tweeting_mps = mps[mps['twitter'].notnull()]
        for index, row in tweeting_mps.iterrows():
            print('Processing {0} of {1}'.format(index, len(tweeting_mps)))

            twitter_info.append(get_twitter_details(row['twitter']))
            time.sleep(5)
        df = pd.DataFrame.from_records(twitter_info)
        df.to_csv('./twitter-info.csv', index=False)
        return df


def get_twitter_friends(twitter_users):
    try: 
        with open('./friends.json', 'r') as fp:
            return json.load(fp)
    except:
        t = {}
        twitter_users_id = set(twitter_users['id'].tolist())
        user_ids_to_username = dict(zip(twitter_users.id, twitter_users.username))
        for index, row in twitter_users.iterrows():
            print('Fetching friends for {0} of {1}'.format(index, len(twitter_users)))
            # get the ids of all the users this user follows
            twitter_friend_ids = [
                _id for _id in api.friends_ids(row['username'])
            ]

            # get the intersection of this users friends with our other users
            intersection = list(twitter_users_id & set(twitter_friend_ids))
            # translate the intersection into usernames
            t[row['username']] = [user_ids_to_username[user_id] for user_id in intersection]
        with open ('./friends.json', 'w') as fp:
            json.dump(t, fp)
    return twitter_friends    


df = get_mps()
if not 'twitter' in df:
    df['twitter'] = df['link'].apply(get_twitter_link)

twitter_info = get_twitter_info(df)
twitter_friends = get_twitter_friends(twitter_info)