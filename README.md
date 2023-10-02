
# <h1 align=center> **Individual Project MLOps** </h1>

<p align="center">
<img src="./_src/mlops_ia.jpg"  height=400 width=400>
</p>
  
<p align="center"> Image generated by <a href="https://creator.nightcafe.studio/">NightCafe</a> with the prompt "machine learning operations individual project banner"</p>
 
# <h2> **Introduction and Objectives of the project** </h2>

In this project i have the goal of transform the provided datasets about steam users and games to find the most valuable variables and be able to create accurate recommendation systems for my API, in which you can also find historical valuable information.

I will divide the process in 3 key tasks:

# <h2> **ETL** </h2>
- Fix nested JSON archives and read them in the right format:
    For this task i used the function `literal_eval` from the `ast` library and `json_normalize` from `pandas`.
- Null values treatment doing web scraping and imputation :
    For this task i used `pandas` for imputation and `bs4` with `requests` for web scraping.
- Look for duplicated and wrong formatted values and drop/transform them.
- Analyze the datasets and drop the columns that aren't useful for the API or the models.
- Transform the review texts into a sentiment value, for this process i did 3 things:
 
    1- Make sure to remove all the unwanted characters with regular expressions (`re`).
  
    2- Translate the reviews to english with `langdetect` and `googletrans` to have a more accurate sentiment analysis.
  
    3- Apply a sentiment analysis pipeline with `hugging face` and handle texts larger than 512 embeddings.
  
- Save the datasets in the right format.
  
To see the process you can visit the [ETL](https://github.com/motm-1/PI_MLOps/blob/main/etl.py) and [Sentiment Analysis](https://github.com/motm-1/PI_MLOps/blob/main/sentiment_analysis.py) archives.

# <h2> **API Development** </h2>

<sub> The format of the examples is input : {output} <sub>

The API consists of 7 endpoints:

- /**PlayTimeGenre**/{`genre`}

This endpoint requires a game genre and returns the year of the release date with the most amount of hours played for the given genre.

Example: `Action` : *{'Year with the most amount of hours played for the genre Action': 2012}*


- /**UserForGenre**/{`genre`}

This endpoint requires a game genre and returns the user with the most amount of hours played for the given genre filtered by year.

Example: `Action` : *{'User with the most amount of hours played for the genre Action':'Sp3ctre', 'Hours Played':[{1993:0}, ..., {2017:43327}]}*


- /**UsersRecommend**/{`year`}

This endpoint requires a year and returns the three most recommended games for the given year.

Example: `2012` : *[{'1':'Team Fortress 2'}, {'2':'Terraria'}, {'3':'Garry's Mod'}]*


- /**UsersNotRecommend**/{`year`}

This endpoint requires a year and returns the three most unrecommended games for the given year.

Example: `2012` : *[{'1':'Call of Duty: Modern Warfare '}, {'2':'Team Fortress 2'}, {'3':'Dota 2'}]*


- /**SentimentAnalysis**/{`year`}

This endpoint requires a year and returns the quantity of reviews categorized by sentiment for the given year.

Example: `2012` : *{'Negative':125, 'Neutral':203, 'Positive':873}*


- /**UserRecommendation**/{`user_id`}

This endpoint requires an user id and returns five recommendations of games similar to the ones that the user and users similars to him likes.

Example: `doctr` : {'Recommendations for the user doctr':['The Wolf Among Us', 'The Witcher 2: Assassins of Kings Enhanced Edition', 'Max Payne 3' 'Defiance', 'The Walking Dead']}


- /**ItemRecommendation**/{`item`}

This endpoint requires a game id or name and returns five recommendations of similar games.

Example: `Killing Floor` : {'Recommendations for the game Killing Floor':['Killing Floor 2', 'Killinf Floor: Uncovered', 'Left 4 Dead 2', 'Resident Evil Revelations / Biohazard Revelations', 'Dead By Daylight']}


To see the process you can visit the [Main](https://github.com/motm-1/PI_MLOps/blob/main/main.py) and [Datasets](https://github.com/motm-1/PI_MLOps/blob/main/build_datasets.py) archives.


# <h2> **EDA And Recommendations Systems** </h2>
- Explore the datasets and find valuable information to make my recommendations systems accurate.
- Create a user-item system recommendation using `TensorFlow`:
  
  This recommendation system utilizes `keras` for constructing the neural network layers and `tensorflow-recommenders` to build the retrieval model responsible for selecting users and games with high similarity.

  The retrieval model operates through a `brute-force` approach. Initially, it generates a list of potential candidates that can be used for similarity comparison. It then takes two parameters, a game, and a user_id, and measures their `similarity` against all the candidates. The system selects the `K most similar` candidates and returns the corresponding items and their scores.
  
  To construct the model, you'll need two essential lists: `unique_users` and `unique_games`, which store data in string format. These lists are the vocabulary for the initial layer of the model. Additionally, you require a games list in tf.Dataset format for the retrieval model.
  
  After creating the model, the next step is to compile it. In this case, I chose Adagrad as the optimizer with an initial learning rate of 0.09. Following compilation, you can fit the model with your datasets.

  I chose the columns `genres` and `tags` and join them to create the column `labels`, transform the elements to strings and concatenate them with the game `title` to create the column that i will use in the games model embedding.
  For the users model embedding i just used the original `user_id` column.

  Now, the last step is to train the model, see if the results are what is expected and download it to be consumed from the API.

- Create a item-item system recommendation using `Scikit-Learn` 
    
  This recommendations system utilizes `TfidVectorizer` from Scikit-Learn to transform our games information into vectors allowing us to calculate similarity scores (using cosine similarity) between them. Once we have a vector for each game in our dataset, we can create the cosine similarity matrix using `linear_kernel`, also from Scikit-Learn. 
    
  The cosine similarity matrix is a [m x m] square matrix, where each item [i, j] represents the similarity between the vector i and the vector j, with values ranging from -1 (opposed vectors) to 1 (identical vectors). This matrix tell us how closely our games are in terms of the selected features.
    
  For this recommendation system I selected the game titles that appear in the reviews dataset since they represent the games people are most interested in, then I chose the columns `labels` and `developer` because they contain the most representative and clean information to ensure the system's accuracy.
