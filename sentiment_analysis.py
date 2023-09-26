import pandas as pd
import numpy as np
import time
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from etl_functions import calc_ejecution_time
from langdetect import detect, LangDetectException
from googletrans import Translator
from requests.exceptions import ReadTimeout
from httpcore._exceptions import ReadTimeout as ReadTimeout2

class SentimentAnalysis:
    """
    A class for performing sentiment analysis on user reviews using a pre-trained model.

    Args:
        model_path (str, optional): The path or name of the pre-trained sentiment analysis model.
            Defaults to 'cardiffnlp/twitter-roberta-base-sentiment-latest'.

    Attributes:
        df (pandas.DataFrame): A DataFrame containing user reviews.
        model_path (str): The path or name of the pre-trained model.
        model (transformers.AutoModelForSequenceClassification): The sentiment analysis model.
        tokenizer (transformers.AutoTokenizer): The model's tokenizer.

    Methods:
        sentiment_analysis: Performs sentiment analysis on user reviews and adds the sentiment scores to the DataFrame.
        classify_large_text: Handles sentiment analysis for long texts.
        set_label: Assigns a sentiment label based on sentiment scores.
        run: Executes the sentiment analysis and saves the results to a CSV file.
    """
    def __init__(self, model_path='cardiffnlp/twitter-roberta-base-sentiment-latest', df_path='CleanDatasets/users_reviews.csv'):
        """
        Initializes the SentimentAnalysis object.

        Args:
            model_path (str, optional): The path or name of the pre-trained sentiment analysis model.
                Defaults to 'cardiffnlp/twitter-roberta-base-sentiment-latest'.
            df_path (str, optional): The path to the CSV file containing user reviews data.
                Defaults to 'Datasets/users_reviews.csv'.
        """
        self.df = pd.read_csv(df_path)
        self.model_path = model_path
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_path)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_path)
    
    def sentiment_analysis(self):
        """
        Perform sentiment analysis on user reviews and update the DataFrame with sentiment labels.
        This method uses a pre-trained sentiment analysis model to classify user reviews into three categories:
        Positive, Neutral, or Negative. The sentiment labels are added to the DataFrame under the 'sentiment_analysis' column.
        If a RuntimeError or IndexError occurs during sentiment analysis, it falls back to using the 'classify_large_text'
        method to handle long texts.
        If a ValueError occurs, a default sentiment label of 1 (Neutral) is assigned.
        """
        self.df.drop(columns='user_url', inplace=True)

        nlp = pipeline('sentiment-analysis', model=self.model, tokenizer=self.tokenizer)

        self.df['sentiment_analysis'] = '--'

        mapping = {
            'positive': 2,
            'neutral': 1,
            'negative': 0
            }

        for i, row in self.df.iterrows():
            text = row.review

            if text == '1':
                self.df.loc[i, 'sentiment_analysis'] = 1
                continue

            try:
                label = nlp(text)[0]['label']
                self.df.loc[i, 'sentiment_analysis'] = mapping.get(label)
            except (RuntimeError, IndexError): # Capture texts with more than 512 words
                self.df.loc[i, 'sentiment_analysis'] = int(self.classify_large_text(text))

        self.df.drop(columns=['review'], inplace=True)
        
    def classify_large_text(self, text):
        """
        Handles sentiment analysis on long texts by dividing them into smaller chunks of text and
        performing sentiment analysis on each chunk. The sentiment scores (positive, neutral, and negative) for
        each chunk are averaged to produce an overall sentiment score for the entire text.

        Args:
            text (str): The long text to analyze.

        Returns:
            int: The sentiment label (0 for Negative, 1 for Neutral, 2 for Positive) based on the averaged scores.
        """
        nlp = pipeline('sentiment-analysis', model=self.model, tokenizer=self.tokenizer, top_k=3)

        tokenized_text = self.tokenizer(text)
        input_ids = tokenized_text['input_ids']

        text_chunks = [input_ids[i:i + 510] for i in range(0, len(input_ids), 510)]
        
        scores = {'pos': [], 'neu': [], 'neg': []}

        for chunk in text_chunks:
            partial_text = self.tokenizer.decode(chunk, skip_special_tokens=True)
            result = nlp(partial_text)[0]
            for i, key in enumerate(scores.keys()):
                scores[key].append(result[i]['score'])
        
        avg_scores = {key: np.mean(val) for key, val in scores.items()}

        return self.set_label(avg_scores)

    def set_label(self, scores):
        """
        Assigns a sentiment label based on sentiment scores.

        Args:
            scores (dict): A dictionary of sentiment scores.

        Returns:
            int: The sentiment label (0 for negative, 1 for neutral, 2 for positive).
        """
        label_index = np.argmax([scores['neg'], scores['neu'], scores['pos']])

        return label_index

    def translate_text(self):
        """
        Translate non-English text reviews in the DataFrame to English.

        This method iterates through the DataFrame's rows, detects the language of each review using the langdetect
        library, and translates non-English reviews to English using the googletrans library. The translated text is
        then updated in the DataFrame.
        """
        translator = Translator()

        for i, row in self.df.iterrows():
            text = row.review
            try:
                detected_language = detect(text)
                if detected_language != 'en':
                    time.sleep(0.3)
                    translated_text = translator.translate(text, src=detected_language, dest='en').text
                    self.df.loc[i, 'review'] = translated_text
            except (LangDetectException, ReadTimeout, TypeError, ReadTimeout2):
                continue
    
    def run(self, save_path):
        """
        Executes the sentiment analysis and saves the results to a CSV file.

        Args:
            save_path (str): The path to save the results CSV file.
        """
        self.translate_text()
        self.sentiment_analysis()
        self.df.to_csv(save_path)
        print('Saved')

@calc_ejecution_time
def main():
    """Execute sentiment analysis functions."""
    save_path = 'ApiDatasets/users_sentiment.csv'
    analysis = SentimentAnalysis()
    analysis.run(save_path)

if __name__ == "__main__":
    main()