import requests
from bs4 import BeautifulSoup

def scrape_genre(soup):
    try:
        genres_sections = soup.find('span', attrs={'data-panel':'{"flow-children":"row"}'}).find_all('a')
        genres = [href.text for href in genres_sections]
        if genres[0] not in ('\nUnder ARS$ 140\n','\nUnder ARS$ 70\n' ):
            return genres
        else:
            return None
    except (AttributeError, TypeError):
        return None

def scrape_tags(soup):
    try:
        tags_sections = soup.find('div', attrs={'class':'glance_tags popular_tags'}).find_all('a')
        tags = [href.text.strip() for href in tags_sections]
        return tags
    except AttributeError:
        return None
    
def scrape_title(soup):
    try:
        title = soup.find('div', attrs={'id':'appHubAppName'}).text
        return title
    except AttributeError:
        return None
    
def scrape_release_date(soup):
    try:
        date = soup.find('div', attrs={'class':'date'}).text
        return date
    except AttributeError:
        return None
    
def scrape_price(soup):
    try:
        price = soup.find('div', attrs={'class':'discount_original_price'})
        if price != None:
            price = price.text.split()[1].replace('.','').replace(',','.')
        else:
            price = soup.find('div', attrs={'class':'game_purchase_action_bg'}).text.strip().split()[1].replace('.','').replace(',','.')
            try:
                price = float(price)
            except (ValueError, TypeError):
                return 0
        return float(price) / 100
    except (AttributeError, IndexError):
        return None

def scrape_developer(soup):
    try:
        developer = soup.find('div', attrs={'id':'developers_list'}).find('a').text
        return developer
    except AttributeError:
        return None
    
def make_requests(url):
    try:
        req = requests.get(url)
        assert req.status_code == 200, f'There is a problem with the url {url}.'
        soup = BeautifulSoup(req.text, 'lxml')
        return soup
    except AssertionError:
        return None

def scrape_missing_row(df):
    for i, row in df[df.isna().any(axis=1)].iterrows():
        url = row.url
        try:
            soup = make_requests(url) 
            if row.genres == None:
                df.loc[i, 'genres'] = f'{scrape_genre(soup)}'
            if row.title == None:
                df.loc[i, 'title'] = scrape_title(soup)
            if row.release_date == None or row.release_date.startswith('Soon'):
                df.loc[i, 'release_date'] = scrape_release_date(soup)
            if row.tags == None:
                df.loc[i, 'tags'] = f'{scrape_tags(soup)}'
            if row.price == None:
                df.loc[i, 'price'] = scrape_price(soup)
            if row.developer == None:
                df.loc[i, 'developer'] = scrape_developer(soup)
        except TypeError:
            continue
    return df