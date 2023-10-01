import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import joblib

def prepare_dataset():
    data = pd.read_parquet('CleanDatasets/collaborative_filtering.parquet')
    data = data.drop_duplicates(subset='title').reset_index(drop=True)
    data['items_data'] = data['labels'].astype('str').apply(lambda x: x.replace('[', '').replace(']', '')) + ' ' + data['developer']
    data = data[['title','item_id','items_data']]
    data['title'] = data['title'].str.title()

    data.to_parquet('ApiDatasets/item_item.parquet')
    return data

def calculate_cosine_sim(data):
    tfidf_vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix = tfidf_vectorizer.fit_transform(data['items_data'])
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)

    joblib.dump(cosine_sim, 'Models/cosine_sim.joblib', compress=True)


def main():
    data = prepare_dataset()
    calculate_cosine_sim(data)

if __name__ == "__main__":
    main()