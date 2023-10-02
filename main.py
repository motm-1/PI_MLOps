from fastapi import FastAPI, HTTPException
import pandas as pd
import joblib

from tensorflow.python.ops.numpy_ops import np_config
np_config.enable_numpy_behavior()
from keras.models import load_model

playtimegenre_df = pd.read_parquet('./_src/ApiDatasets/playtimegenre.parquet')
userforgenre_df = pd.read_parquet('./_src/ApiDatasets/userforgenre.parquet')
usersrecommend_df = pd.read_parquet('./_src/ApiDatasets/usersrecommend.parquet')
usersnotrecommend_df = pd.read_parquet('./_src/ApiDatasets/usersnotrecommend.parquet')
sentimentanalysis_df = pd.read_parquet('./_src/ApiDatasets/sentimentanalysis.parquet')
item_item_df = pd.read_parquet('./_src/ApiDatasets/item_item.parquet')
cosine_sim = joblib.load('./_src/Models/cosine_sim.joblib')
model = load_model('./_src/Models/collaborative_filtering')

app = FastAPI()

@app.get('/PlayTimeGenre/{genre}')
async def playtimegenre(genre:str):
    genre = genre.title()

    df = playtimegenre_df[playtimegenre_df['genres'] == genre]

    try:
        df.iloc[0]
    except IndexError: #Nonexistent genre
        raise HTTPException(status_code=404, detail=f"The genre {genre} doesn't exists")
    
    return {f'Year with the most amount of hours played for the genre {genre}': int(df["release_date"].values[0])}

@app.get('/UserForGenre/{genre}')
async def userforgenre(genre:str):
    genre = genre.title()

    df = userforgenre_df[userforgenre_df['genres'] == genre]
    list_years_hours = df[['release_date', 'playtime_forever']].to_numpy().tolist()

    try:
        df.iloc[0]
    except IndexError: #Nonexistent genre
        raise HTTPException(status_code=404, detail=f"The genre {genre} doesn't exists")
    
    return {f'User with the most amount of hours played for the genre {genre}': df["user_id"].iloc[0],'Hours Played': [{int(key):value} for key, value in list_years_hours]}

@app.get('/UsersRecommend/{year}')
async def usersrecommend(year:int):
    df = usersrecommend_df[usersrecommend_df['year'] == year]
    position_game = df[['position', 'title']].to_numpy().tolist()

    try:
        position_game[0]
    except IndexError: #Nonexistent year
        raise HTTPException(status_code=404, detail=f"The year {year} doesn't exists in our database")
    
    return [{position: f"{title}"} for position, title in position_game]

@app.get('/UsersNotRecommend/{year}')
async def usersnotrecommend(year:int):
    df = usersnotrecommend_df[usersnotrecommend_df['year'] == year]
    position_game = df[['position', 'title']].to_numpy().tolist()

    try: 
        position_game[0]
    except IndexError: #Nonexistent year
        raise HTTPException(status_code=404, detail=f"The year {year} doesn't exists in our database")

    return [{position: f"{title}"} for position, title in position_game]

@app.get('/SentimentAnalysis/{year}')
async def sentimentanalysis(year:int):
    labels = {0:'Negative', 1:'Neutral', 2:'Positive'}
    df = sentimentanalysis_df[sentimentanalysis_df['year'] == year]
    df.reset_index(drop=True, inplace=True)

    try:
        df.loc[0]
    except (ValueError, KeyError): #Nonexistent year
        raise HTTPException(status_code=404, detail=f"The year {year} doesn't exists in our database")
    
    return {labels[key]:value for key, value in zip(df['sentiment_analysis'].tolist() ,df['count'].tolist())}

@app.get('/UserRecommendation/{user_id}')
async def userrecommendation(user_id:str):

    scores, titles = model([user_id])
    return{f"Recommendations for the user {user_id}": [title.decode('utf-8').split(',')[0] for title in titles[0,:5].tolist()]}

@app.get('/ItemRecommendation/{item}')
async def itemrecommendation(item:str):
    data = item_item_df.copy()

    try:
        try:
            item = int(item)
            item = data.loc[data['item_id'] == item,'title'].values[0]
        except ValueError:
            item = item.title()
        idx = data.index[data['title'] == item].tolist()[0]
    except IndexError:
        raise HTTPException(status_code=404, detail=f"The game {item} doesn't exists in our database")
    
    idx = data.index[data['title'] == item].tolist()[0]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:6]
    game_indices = [i[0] for i in sim_scores]

    recommendations = [data["title"].iloc[i].strip() for i in game_indices]

    return {f'Recommendations for the game {item}': recommendations}