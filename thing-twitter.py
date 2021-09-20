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
from bubblechart import BubbleChart


mp_url = 'https://www.althingi.is/thingmenn/althingismenn/'
footer_text = '@karltryggvason - 09.2021 / heimildir: althingi.is og twitter.com'

auth = tweepy.OAuthHandler(os.environ.get('consumer_key', ''), os.environ.get('consumer_secret', ''))
auth.set_access_token(os.environ.get('access_token', ''), os.environ.get('access_token_secret', ''))
api = tweepy.API(auth, wait_on_rate_limit=True)


sns.set_theme(style='white', font_scale=0.75, font='Helvetica')
background_colour = '#2d5382'
sns.set(rc={
    'axes.facecolor': background_colour, 'figure.facecolor': background_colour, 
    'axes.edgecolor': '#ffffff', 'patch.edgecolor': '#ffffff',
    'axes.labelcolor': '#ffffff', 'text.color': '#ffffff', 'ytick.color': '#ffffff'
})

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


the_government = [
    "Framsóknarflokkur",
    "Sjálfstæðisflokkur",
    "Vinstrihreyfingin - grænt framboð"
]


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
        return pd.read_csv('./twitter-info.csv', parse_dates=['created_at'])
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


def count_by_party(df, attribute, name):
    by_party = df.groupby('party')
    agg = by_party.agg({attribute: ['sum', 'mean', 'median']})
    
    # shamelessly "inspired" by https://seaborn.pydata.org/examples/palette_choices.html
    # Set up the matplotlib figure
    f, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 12), sharex=True, facecolor=background_colour)
    f.figure.suptitle('Fjöldi {} eftir flokkum'.format(name))

    sns.barplot(x=agg.index, y=agg[attribute]['sum'], palette=party_colours, ax=ax1)
    ax1.set_ylabel('Samtals'.format(name), color='#ffffff')
    ax1.set_xlabel('')

    sns.barplot(x=agg.index, y=agg[attribute]['median'], palette=party_colours, ax=ax2)
    ax2.set_ylabel('Miðgildi Þingmanna', color='#ffffff')
    ax2.set_xlabel('')
    ax2.spines['bottom'].set_color('#ffffff')

    sns.barplot(x=agg.index, y=agg[attribute]['mean'], palette=party_colours, ax=ax3)
    ax3.set_ylabel('Meðaltal á Þingmann', color='#ffffff')
    ax3.set_xlabel('')

    labels = [
        '\n' + l if i % 2 != 0 else l for i, l in enumerate(agg.index)
    ]
    ax3.set_xticklabels(labels, color='#ffffff')

    # Finalize the plot
    sns.despine(bottom=True)
    plt.setp(f.axes)
    plt.tight_layout(h_pad=2, rect=(1, 1, 1, 1))
    f.text(0, 0, footer_text, va='bottom', color='#ffffff')
    plt.savefig('{0}-by-party.png'.format(attribute))


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
    plt.rcParams['hatch.linewidth'] = 0.4
    ax.set_ylabel('Fjöldi', color='#ffffff')
    ax.set_xticks(x)
    labels = [
        '\n' + l if i % 2 != 0 else l for i, l in enumerate(data.index)
    ]
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.set_xticklabels(labels, color='#ffffff')

    ax.legend(fontsize='large', markerscale=10, loc='best', edgecolor=background_colour)
    
    ax.grid(False, axis='x')
    fig.text(0, 0, footer_text, va='bottom')
    fig.suptitle('Fjöldi Þingmanna og fjöldi tístandi þingmanna eftir flokkum')
    plt.setp(fig.axes)
    plt.tight_layout(h_pad=2, rect=(1, 1, 1, 1))
    plt.savefig('party-twitter-users.png')


def government_twitter_users(df):
    df['government']= df['party'].apply(lambda x: x in the_government)
    data = df.groupby('government').agg(
        {'followers_count': 'sum'}
    )

    width = 0.5  # the width of the bars
    fig, ax = plt.subplots(figsize=(12, 12))
    ax.bar(['Stjórnarandstaða', 'Stjórn'], data.followers_count, width,
        label='Fylgjendur tístandi Þingmanna', color=['#1c79c2', '#1c79c2']
    )
    
    ax.set_ylabel('Fjöldi fylgjenda', color='#ffffff')
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.set_xticklabels(['Stjórnarandstaða', 'Stjórn'], color='#ffffff')

    ax.legend(fontsize='large', markerscale=10, loc='best', edgecolor=background_colour)
    
    ax.grid(False, axis='x')
    fig.text(0, 0, footer_text, va='bottom')
    fig.suptitle('Fjöldi Twitter fylgjanda þingmanna í stjórn og stjórnarandstöðu')
    plt.setp(fig.axes)
    plt.tight_layout(h_pad=2, rect=(1, 1, 1, 1))
    plt.savefig('followers-by-government.png')


def mp_twitter_scatterchart(df):
    df['color'] = df['party'].apply(lambda x: party_colours[x])
    df = df.sort_values(by=['party'])

    def format_name(row):
        return '@{0}\n{1}k'.format(row.username, round(row.followers_count/1000, 1))
    
    bubble_chart = BubbleChart(area=df['followers_count'], bubble_spacing=1)
    bubble_chart.collapse()
    f, ax = plt.subplots(subplot_kw=dict(aspect="equal"), figsize=(12, 12), facecolor=background_colour)

    annotations = df.apply(format_name, axis=1)

    bubble_chart.plot(ax, annotations, df['color'])
    
    ax.axis("off")
    ax.relim()
    ax.autoscale_view()

    f.text(0, 0, footer_text, va='bottom', color='#ffffff')
    f.suptitle('Þingmenn eftir fjölda Twitter Fylgjenda', color='#ffffff', fontsize="large")
    plt.tight_layout(pad=0)
    plt.savefig('mp-twitter-users-follower-counts.png')


def mp_tweets_scatterchart(df):
    df['color'] = df['party'].apply(lambda x: party_colours[x])
    df = df.sort_values(by=['party'])
    
    def format_name(row):
        return '@{0}\n{1}k'.format(row.username, round(row.statuses_count/1000, 1))

    bubble_chart = BubbleChart(area=df['statuses_count'], bubble_spacing=1)
    bubble_chart.collapse(5)
    f, ax = plt.subplots(subplot_kw=dict(aspect="equal"), figsize=(12, 12), facecolor=background_colour)

    annotations = df.apply(format_name, axis=1)

    bubble_chart.plot(ax, annotations, df['color'])
    
    ax.axis("off")
    ax.relim()
    ax.autoscale_view()

    f.text(0, 0, footer_text, va='bottom', color='#ffffff')
    f.suptitle('Þingmenn eftir fjölda tísta', color='#ffffff', fontsize="large")
    plt.tight_layout(pad=0)
    plt.savefig('mp-twitter-users-status-counts.png')


df = get_mps()
if not 'twitter' in df:
    df['twitter'] = df['link'].apply(get_twitter_link)

twitter_info = get_twitter_info(df)
twitter_friends = get_twitter_friends(twitter_info)
joined = join_frames(df, twitter_info)
count_by_party(joined, 'followers_count', 'twitter fylgjanda')
count_by_party(joined, 'statuses_count', 'tísta')
party_twitter_users(df)
government_twitter_users(joined)
mp_twitter_scatterchart(joined)
mp_tweets_scatterchart(joined)