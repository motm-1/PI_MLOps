import pandas as pd
import numpy as np
import re
from etl_functions import read_json_file, handle_price_exceptions, set_datetime, calc_ejecution_time, parse_lists, convert_html
from scraping_functions import scrape_missing_row

@calc_ejecution_time
def reviews_datasets(filename='Datasets/australian_user_reviews.json', return_original=False):
    """
    Process a dataset of user reviews about videogames from a JSON file, and save it as a CSV file.

    Parameters:
        filename (str, optional): The name of the JSON file containing user reviews data. 
            Defaults to 'Datasets/australian_user_reviews.json'.
        return_original (bool, optional): Whether to return the original DataFrame.
            Defaults to False.

    Returns:
        None or pandas.DataFrame: If 'return_original' is True, returns the original DataFrame without datetime dates

    Raises:
        FileNotFoundError: If the specified JSON file does not exist.
    """
    correct_json = read_json_file(filename)
    df_reviews = pd.json_normalize(correct_json, record_path='reviews', meta='user_id')

    columns_order = ['user_id'] + [col for col in df_reviews.columns if col not in ('user_id',)]
    df_reviews = df_reviews.reindex(columns=columns_order)

    if return_original == True:
        return df_reviews

    df_reviews['posted'] = set_datetime(df_reviews['posted'])

    df_reviews.drop(columns=['funny', 'last_edited', 'helpful'], inplace=True) # Useless columns
    df_reviews.drop_duplicates(subset=['user_id', 'item_id', 'review'], inplace=True)

    pattern = r'[^\w\s\':)(]+' #Regex to remove special characters in order to get a precise sentiment analysis
    df_reviews['review'] = df_reviews['review'].apply(lambda x: re.sub(pattern, '', x))

    df_reviews.loc[df_reviews['review'].str.strip() == '', 'review'] = '1' # Set null reviews to neutral
    
    df_reviews.to_csv('CleanDatasets/users_reviews.csv', index=False)

@calc_ejecution_time
def users_items_dataset(filename='Datasets/australian_users_items.json'):
    """
    Process a dataset of steam users data from a JSON file and save it as a CSV file.

    Parameters:
        filename (str, optional): The name of the JSON file containing user items data. 
            Defaults to 'Datasets/australian_users_items.json'.
        return_ (bool, optional): Whether to return the processed DataFrame instead of saving it as a CSV file.
            Defaults to False.
        
    Raises:
        FileNotFoundError: If the specified JSON file does not exist.
    """
    correct_json = read_json_file(filename)
    df_users_items = pd.json_normalize(correct_json, record_path='items', meta='user_id')
    df_users_items.drop('playtime_2weeks', axis=1, inplace=True) # Useless column
    df_users_items.rename(columns={'item_id':'id'}, inplace=True)

    df_users_items.to_csv('CleanDatasets/users_items.csv', index=False)

@calc_ejecution_time
def steam_games_dataset(filename='Datasets/steam_games.json.gz', return_original=False):
    """
    Process a dataset of Steam games from a compressed JSON file and save it as a CSV file.

    Parameters:
        filename (str, optional): The name of the compressed JSON file containing Steam games data. 
            Defaults to 'Datasets/steam_games.json.gz'.

    Returns:
        pandas.DataFrame or None: If 'return_original' is True, returns the processed DataFrame. Otherwise, saves it as 'steam_games.csv'

    Raises:
        FileNotFoundError: If the specified compressed JSON file does not exist.
    """
    df_games = pd.read_json(filename, lines=True, compression='gzip')
    df_games.dropna(thresh=5, inplace=True)
    df_games.drop(columns=['reviews_url', 'specs', 'early_access','app_name','publisher'], inplace=True) # Useless columns
    df_games.reset_index(drop=True, inplace=True)

    if return_original == True:
        return df_games

    df_games['id'].fillna(df_games['url'].str.extract(r'([0-9]+)')[0].astype('float'), inplace=True)
    df_games['id'] = df_games['id'].astype(int) # Remove dots
    df_games.drop_duplicates(subset='id', inplace=True)

    df_games['title'].fillna(df_games['url'].apply(lambda x: x.split('/')[-2].replace('_', ' ').replace('  ', ' ')), inplace=True)
    # Extract title from url

    df_games = df_games.replace(['', ' ', '  '], np.nan)

    df_games = scrape_missing_row(df_games) # Web scraping
    df_games = df_games.replace('', np.nan)

    df_games['price'].fillna(0, inplace=True)
    df_games['price'] = df_games['price'].apply(handle_price_exceptions) # Clean price column

    df_games['genres'] = df_games['genres'].apply(parse_lists).apply(convert_html)
    # Convert strings to lists and convert html characters to unicode
    df_games['tags'] = df_games['tags'].apply(parse_lists).apply(convert_html) 

    df_games['release_date'] = pd.to_datetime(df_games['release_date'], format='mixed', errors='coerce')

    df_games.to_csv('CleanDatasets/steam_games.csv', index=False)

def main(): 
    """Execute data processing functions."""
    reviews_datasets()
    users_items_dataset()
    steam_games_dataset()

if __name__ == "__main__":
    main()