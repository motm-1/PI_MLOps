import pandas as pd
import numpy as np
import tensorflow as tf
import tensorflow_recommenders as tfrs
from tensorflow.python.ops.numpy_ops import np_config
from typing import Dict, Text

path = 'Models/collaborative_filtering'

np_config.enable_numpy_behavior()

class GamesModel(tfrs.Model):
    """A recommendation model for suggesting games to users based on their interactions.

    This model is designed for building a collaborative filtering recommendation system using
    TensorFlow Recommenders (TFRS). It learns embeddings for game titles and user IDs to
    make game recommendations based on user interactions.

    Parameters:
        unique_games_ids (list): A list of unique games.
        unique_users_ids (list): A list of unique user IDs.

    Attributes:
        games_model (tf.keras.Model): A sequential model for learning game embeddings.
        user_model (tf.keras.Model): A sequential model for learning user embeddings.
        task (tfrs.tasks.Retrieval): A retrieval task for calculating recommendation loss.
        
    Methods:
        compute_loss(features, training=False):
            Computes the loss for training the recommendation model.
    
    Author:
        [Tomas Filippo]
    Date:
        [09/29/23]     
    """
    def __init__(self, unique_games_ids, unique_users_ids, games):
        super().__init__()

        self.unique_games_ids = unique_games_ids

        self.unique_users_ids = unique_users_ids

        self.games = games

    # Define the 'games_model' and 'user_model' which learns embeddings for game titles and users IDs.
    # It uses two layers:
    # 1. 'StringLookup' layer converts data to integer indices.
    # 2. 'Embedding' layer creates dense embeddings for these data.

        self.games_model: tf.keras.Model = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=self.unique_games_ids, mask_token=None),
            tf.keras.layers.Embedding(
                len(self.unique_games_ids) + 1, output_dim=16)])
        
        self.user_model: tf.keras.Model = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=self.unique_users_ids, mask_token=None),
            tf.keras.layers.Embedding(
                len(self.unique_users_ids) + 1, output_dim=16)])
        
    # This task is responsible for retrieving top-k game recommendations.
    # It uses the 'FactorizedTopK' metric to evaluate recommendations.
    # The candidates are generated by batching and mapping game data through 'games_model'.
      
        self.task: tf.keras.layers.Layer = tfrs.tasks.Retrieval(
            metrics= tfrs.metrics.FactorizedTopK(
                candidates = self.games.batch(128).map(self.games_model)))
    
    def compute_loss(self, features: Dict[Text, tf.Tensor], training=False) -> tf.Tensor:
        """Compute the loss for training the recommendation model.

        Parameters:
            features (Dict[Text, tf.Tensor]): A dictionary of input features, including "user_id" and "title".
            training (bool): A flag indicating whether it's training or inference mode.

        Returns:
            tf.Tensor: The computed loss.
        """
        
        #Create user embeddings
        user_embeddings = self.user_model(features["user_id"])

        #Create positive game embeddings (games that the user has and rated)
        positive_game_embeddings = self.games_model(features["title"])

        return self.task(user_embeddings, positive_game_embeddings)

def train_model(data:dict):
    #Convert data to tensorflow dataset
    data = tf.data.Dataset.from_tensor_slices(data)
    #Change dataset keys names to be more specific
    dataset = dataset.map(lambda x:
                    {'user_id':x['user_id'],
                    'title':x['to_recommend']})
    
    games = dataset.map(lambda x: x['title'])

    #Set a random seed to shuffle our dataset and separate the train data
    tf.random.set_seed(42)
    shuffled = dataset.shuffle(100_000, seed=42, reshuffle_each_iteration=True)
    train = shuffled.take(40_000)

    #Create the vocabulary for our Embeddings layers
    games_ids = dataset.batch(1_000_000).map(lambda x: x["title"])
    unique_games_ids = np.unique(np.concatenate(list(games_ids)))

    users_ids = dataset.batch(1_000_000).map(lambda x:x['user_id'])
    unique_users_ids = np.unique(np.concatenate(list(users_ids)))

    #Instantiate the model and compile with an adaptative learning rate of 0.09
    model = GamesModel(unique_games_ids, unique_users_ids, games)
    model.compile(optimizer = tf.keras.optimizers.Adagrad(learning_rate=0.09))

    #Divide the data in batches of 8192 instances
    cached_train = train.shuffle(100_000).batch(8192).cache()

    model.fit(cached_train, epochs=2)

    # Create a search index to make efficient recommendations based on BruteForce (similarity between users)
    index = tfrs.layers.factorized_top_k.BruteForce(model.user_model)

    index.index_from_dataset(dataset.batch(100).map(lambda x: (x['title'], model.user_model(x['user_id']))))

    index.save(path)

    print('Model Saved')

def list_to_str(list):
    return ', '.join(list)

def main():
    df_s = pd.read_parquet('CleanDatasets/collaborative_filtering.parquet')
    # Select only positive reviews
    df_s = df_s[df_s['sentiment_analysis'] == 2]
    df_s['labels_2'] = df_s['labels'].apply(list_to_str)
    # Merge the games titles and labels to get more accurate recommendations
    df_s['to_recommend'] = df_s['title'] + ', ' + df_s['labels_2']
    data = {name:value.tolist() for name, value in df_s[['user_id','to_recommend']].items()}
    train_model(data)

if __name__ == "__main__":
    main()