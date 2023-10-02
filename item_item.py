import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import joblib

def prepare_dataset():
    """
    Removes duplicate entries based on 'title', preprocesses the 'labels' and 'developer'
    columns to create 'items_data', and saves the resulting dataset to a new Parquet file.
    """
    data = pd.read_parquet('CleanDatasets/collaborative_filtering.parquet')
    data = data.drop_duplicates(subset='title').reset_index(drop=True)
    data['items_data'] = data['labels'].astype('str').apply(lambda x: x.replace('[', '').replace(']', '')) + ' ' + data['developer']
    data = data[['title','item_id','items_data']]
    data['title'] = data['title'].str.title()

    data.to_parquet('_src/ApiDatasets/item_item.parquet')
    
    return data

def calculate_cosine_sim(data):
    """
    Computes the TF-IDF vectorization and cosine similarity matrix for the 'items_data'
    column of the input dataset. The resulting cosine similarity matrix is then saved
    using joblib to '_src/Models/cosine_sim.joblib'.

    Parameters:
    data (pandas.DataFrame): The dataset containing the 'items_data' column.
    """
    tfidf_vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix = tfidf_vectorizer.fit_transform(data['items_data'])
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)

    joblib.dump(cosine_sim, '_src/Models/cosine_sim.joblib', compress=True)

def main():
    data = prepare_dataset()
    calculate_cosine_sim(data)

if __name__ == "__main__":
    main()