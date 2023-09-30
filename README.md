# <h1> **Individual Project MLOps** </h1>
<p align="center">
<img src="./_src/mlops_ia.jpg"  height=400 width=400>
</p>
  
<p align="center"> Image created by <a href="https://creator.nightcafe.studio/">NightCafe</a> </p>
 
# <h2> **Introduction and Objetives of the project** </h2>

In this project i have the objective of transform the datasets provided and find the most valuable variables to make accurate recommendation systems for my API, in which you can also find historical information about steam games and users.

I will divide the process in 4 key tasks:

# <h2> **ETL** </h2>
- Fix nested JSON archives and read them in the right format.
- Null values treatment doing web scraping and imputation.
- Look for duplicated and wrong formatted values and drop/transform them.
- Analyze the datasets and drop useless information.
- Transform the review texts into a sentiment value, for this process i did 3 things:
 
    1- Make sure to remove all the unwanted characters.
  
    2- Translate the data to english to have a more accurate analysis.
  
    3- Apply a sentiment analysis pipeline and handle texts larger than 512 embeddings.
  
- Save the datasets in the right format.
  
To see the process you can visit the [ETL](https://github.com/motm-1/PI_MLOps/blob/main/etl.py) and [Sentiment Analysis](https://github.com/motm-1/PI_MLOps/blob/main/sentiment_analysis.py) archives.

# <h2> **API Development** </h2>
The API consist of 7 endpoints:

- /**PlayTimeGenre**/{`genre`}

This endpoint requires a game genre and returns the year of the release date with the most hours played for the given genre.

- /**UserForGenre**/{`genre`}

This endpoint requires a game genre and returns the user with the most hours played for the given genre filtered by year.

- /**UsersRecommend**/{`year`}

This endpoint requires a year and returns the three most recommended games for the given year.

- /**UsersNotRecommend**/{`year`}

This endpoint requires a year and returns the three most unrecommended games for the given year.

- /**SentimentAnalysis**/{`year`}

This endpoint requires a year and returns the quantity of reviews categorized by sentiment for the given year.
