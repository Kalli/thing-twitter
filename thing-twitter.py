import requests
from bs4 import BeautifulSoup
import pandas as pd

mp_url = 'https://www.althingi.is/thingmenn/althingismenn/'


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


df = get_mps()
if not 'twitter' in df:
    df['twitter'] = df['link'].apply(get_twitter_link)


df.to_csv('./mps.csv')
