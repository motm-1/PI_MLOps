import ast, re, time, html
from datetime import datetime

def read_json_file(filename):
    """
    Transforms a text file containing JSON-formatted data into a list of dictionaries.

    Parameters:
        filename (str): The name of the text file to process.

    Returns:
        list: A list of dictionaries representing the data from the JSON file.

    Raises:
        FileNotFoundError: Raised if the specified file is not found.
        ValueError: Raised if the file contains lines that are not valid JSON.
    """
    dicts = []

    with open(filename, 'r', encoding='utf-8') as f:
        data = f.readlines()
    
    for line in data:
        dicts.append(ast.literal_eval(line))
        
    return dicts

def set_datetime(dates):
    """
    Convert a list of date strings to datetime objects.
    This function takes a list of date strings in the format 'Posted Month Day, Year.' or 'Posted Month Day.' and
    converts them into datetime objects. It handles cases where the year is missing by using a default year of 2016.

    Parameters:
        dates (list): A list of date strings in the format 'Posted Month Day, Year.' or 'Posted Month Day.'.

    Returns:
        list: A list of datetime objects corresponding to the input dates.
    """
    months = { 
    'January': 1, 'February': 2, 'March': 3,
    'April': 4, 'May': 5, 'June': 6,
    'July': 7, 'August': 8, 'September': 9,
    'October': 10, 'November': 11, 'December': 12
    } # To map month names to their numerical values.

    parsed_dates = []

    for date in dates:
        words = date.split()
        month = months[words[1]]
        day = int(words[2][:-1])
        try:
            year = int(words[3][:-1])
        except IndexError: # Handle Missing Year
            year = 2016
        
        date_obj = datetime(year, month, day)
        parsed_dates.append(date_obj)

    return parsed_dates

def handle_price_exceptions(price):
    """
    Processes the price and attempts to handle exceptions related to price representations.
    It converts valid price strings to their original format and sets invalid or exceptional price strings to '0'.

    Parameters:
        prices (str): A price value represented as string, including potential exceptions.
    
    Returns:
        str: A cleaned price value as string, with exceptions resolved.

    Raises:
        AttributeError: If a 'price' is not a string, causing an issue with the 'split' method.
    """
    try: # Verify if the element is already a clean price
        float(price)
        return price
    except ValueError:
        if price.split(' ')[0] == 'Starting':
            price = re.search(r'([\d]+(?:\.\d{2})?)', price).group(1) # Extract the numeric part from strings like 'Starting at $9.99'
            return price
        else:
            return 0 # Set other exceptional price representations to 0

def parse_lists(genre_list):
    """
    Parse a string containing a list in string format into a regular list.

    Parameters:
    genre_list (str): A string representing a list of genres.

    Returns:
    list or None: If successfully parsed, returns a list of genres. If parsing fails, returns None.
    """
    try:
        return ast.literal_eval(genre_list)
    except (ValueError, SyntaxError):
        return genre_list
    
def convert_html(list_):
    """
    Convert a list of strings containing HTML escape sequences to plain text.

    Parameters:
        list_ (list): A list of strings that may contain HTML escape sequences.

    Returns:
        list: A new list of strings with HTML escape sequences converted to plain text.
    """
    try:
        for i, element in enumerate(list_):
            list_[i] = html.unescape(element)
        return list_
    except (AttributeError, TypeError):
        return list_
    
def calc_ejecution_time(func):
    """Decorator to measure the execution time of a function."""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        time_elapsed = end - start
        time_elapsed = round(time_elapsed, 2)
        print(f"Time elapsed in '{func.__name__}': {time_elapsed} seconds")
        return result
    return wrapper
