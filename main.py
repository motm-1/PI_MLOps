from fastapi import FastAPI, HTTPException
import pandas as pd

playtimegenre_df = pd.read_parquet('ApiDatasets/playtimegenre.parquet')
userforgenre_df = pd.read_parquet('ApiDatasets/userforgenre.parquet')
usersrecommend_df = pd.read_parquet('ApiDatasets/usersrecommend.parquet')
usersnotrecommend_df = pd.read_parquet('ApiDatasets/usersnotrecommend.parquet')

app = FastAPI()

@app.get('/PlayTimeGente/{genre}')
async def playtimegenre(genre:str):
    genre = genre.title()

    df = playtimegenre_df[playtimegenre_df['genres'] == genre]

    try:
        df.iloc[0]
    except IndexError: #Nonexistent genre
        raise HTTPException(status_code=404, detail=f"The genre {genre} doesn't exists")
    
    return {f'Year with the most played hours for the {genre} genre: {df["release_date"].values[0]}'}

@app.get('/UserForGenre/{genre}')
async def userforgenre(genre:str):
    genre = genre.title()

    df = userforgenre_df[userforgenre_df['genres'] == genre]
    list_years_hours = df[['release_date', 'playtime_forever']].to_numpy().tolist()

    try:
        df.iloc[0]
    except IndexError: #Nonexistent genre
        raise HTTPException(status_code=404, detail=f"The genre {genre} doesn't exists")
    
    return {f'User with the most hours for the genre {genre}: {df["user_id"].iloc[0]}', f'{[{key:value} for key, value in list_years_hours]}'}

@app.get('/UsersRecommend/{year}')
async def usersrecommend(year:int):
    df = usersrecommend_df[usersrecommend_df['year'] == year]
    position_game = df[['position', 'title']].to_numpy().tolist()

    try:
        position_game[0]
    except IndexError: #Nonexistent year
        raise HTTPException(status_code=404, detail=f"The year {year} doesn't exists in our database")
    
    return [{position: f"{title}" for position, title in position_game}]

@app.get('/UsersNotRecommend/{year}')
async def usersnotrecommend(year:int):
    df = usersnotrecommend_df[usersnotrecommend_df['year'] == year]
    position_game = df[['position', 'title']].to_numpy().tolist()
    
    try:
        position_game[0]
    except IndexError: #Nonexistent year
        raise HTTPException(status_code=404, detail=f"The year {year} doesn't exists in our database")

    return [{position: f"{title}" for position, title in position_game}]