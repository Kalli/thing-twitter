from typing import OrderedDict
import requests
from bs4 import BeautifulSoup
import pandas as pd
import tweepy
import json 
import time
import os
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


mp_url = 'https://www.althingi.is/thingmenn/althingismenn/'


auth = tweepy.OAuthHandler(os.environ.get('consumer_key', ''), os.environ.get('consumer_secret', ''))
auth.set_access_token(os.environ.get('access_token', ''), os.environ.get('access_token_secret', ''))
api = tweepy.API(auth, wait_on_rate_limit=True)


sns.set_theme(style="whitegrid", font_scale=0.75, font='Helvetica')
party_colours = OrderedDict({
    "Flokkur fólksins": "#FFCA3E",
    "Framsóknarflokkur": "#00683F",
    "Miðflokkurinn": "#002169",
    "Píratar": "#7b68ee",
    "Samfylkingin": "#ea0138",
    "Sjálfstæðisflokkur": "#00adef",
    "Vinstrihreyfingin - grænt framboð": "#217462",
    "Viðreisn": "#FF7D14",
})

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


def join_frames(df, twitter_info):
    twitter_info = twitter_info.set_index('twitter_link')
    df = df.set_index('twitter')
    return twitter_info.join(df, lsuffix='_twitter', rsuffix='_mp')


def followers_by_party(df):
    by_party = df.groupby('party')
    agg = by_party.agg({'followers_count': ['sum', 'mean', 'median']})
    
    # shamelessly "inspired" by https://seaborn.pydata.org/examples/palette_choices.html
    # Set up the matplotlib figure
    f, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 12), sharex=True)
    f.figure.suptitle('Þá tísti Þingheimur - Twitter tölfræði alþingismanna')

    sns.barplot(x=agg.index, y=agg['followers_count']['sum'], palette=party_colours, ax=ax1)
    ax1.axhline(color="k", clip_on=False)
    ax1.set_ylabel("Heildar fjöldi fylgjanda")
    ax1.set_xlabel("")

    sns.barplot(x=agg.index, y=agg['followers_count']['median'], palette=party_colours, ax=ax2)
    ax2.axhline(color="k", clip_on=False)
    ax2.set_ylabel("Miðgildi fjölda fylgjanda á þingmann")
    ax2.set_xlabel("")

    sns.barplot(x=agg.index, y=agg['followers_count']['mean'], palette=party_colours, ax=ax3)
    ax3.axhline(color="k", clip_on=False)
    ax3.set_ylabel("Meðalfjöldi fylgjanda á þingmann")
    ax3.set_xlabel("Fjöldi twitter fylgjanda eftir flokkum")
    ax3.set_xticklabels(agg.index, va='top')

    # Finalize the plot
    sns.despine(bottom=True)
    plt.setp(f.axes)
    plt.tight_layout(h_pad=2, rect=(1, 1, 1, 1))
    f.text(0, 0, '@karltryggvason - 2021 / heimildir: althingi.is og twitter.com', va='bottom')
    plt.savefig('followers-by-party.png')


def party_twitter_users(df):
    data = df.groupby('party').agg(
        {'twitter': 'count', 'name': 'count'}
    )
    data.diff = data.name - data.twitter
    x = np.arange(len(data.index))  # the label locations
    width = 0.5  # the width of the bars

    fig, ax = plt.subplots(figsize=(12, 12))
    ax.bar(data.index, data.twitter, width, label='Fjöldi Þingmanna á Twitter', color=party_colours.values(), hatch='///')
    ax.bar(data.index, data.diff, width, label='Fjöldi Þingmanna', bottom=data.twitter, color=party_colours.values())
    plt.rcParams['hatch.linewidth'] = 0.3
    ax.set_ylabel('Fjöldi')
    ax.set_title('Fjöldi Þingmanna og fjöldi þingmanna á Twitter eftir flokkum')
    ax.set_xticks(x)
    labels = [
        '\n' + l if i % 2 != 0 else l for i, l in enumerate(data.index)
    ]
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.set_xticklabels(labels)
    ax.legend(fontsize='large', markerscale=2)
    ax.grid(False, axis="x")
    fig.text(0, 0, '@karltryggvason - 2021 / heimildir: althingi.is og twitter.com', va='bottom')
    fig.suptitle('Þá tísti Þingheimur - Twitter tölfræði alþingismanna')
    plt.setp(fig.axes)
    plt.tight_layout(h_pad=2, rect=(1, 1, 1, 1))
    plt.savefig('party-twitter-users.png')
    plt.show()


df = get_mps()
if not 'twitter' in df:
    df['twitter'] = df['link'].apply(get_twitter_link)

twitter_info = get_twitter_info(df)
twitter_friends = get_twitter_friends(twitter_info)
joined = join_frames(df, twitter_info)
followers_by_party(joined)
party_twitter_users(df)