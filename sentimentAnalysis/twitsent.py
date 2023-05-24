import torch
import os
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from database.database import Database

class TwitterSentimal:
    CSV_DIR = "/sentiment_output.csv"
    INTENSE_CSV_DIR = "/intense_sentiment_output.csv"
    def __init__(self, db: Database, datadir):
        self.CSV_PATH = datadir + self.CSV_DIR
        self.INTENSE_CSV_PATH = datadir + self.INTENSE_CSV_DIR
        self.db = db
        self.status = 0
        self.count = 0
    
    def sentiment_score(self, review):
        tokens = self.tokenizer.encode(review, return_tensors='pt').cuda()
        result = self.model(tokens)
        self.status += 1
        sentiment = int(torch.argmax(result.logits))
        print(str(self.status) + "/" + str(self.count) + ": " + str(sentiment))
        return sentiment
        
    def to_db(self, rating, tweet_id):
        self.db.cur.execute("""
            UPDATE tweet
            SET ai_rating =?
            WHERE id =?
            """,
            (rating,
            tweet_id,)     
        )
    
    def tweet_string(self, tweet, movie):
        movie_name = []
        for word in movie.split(' '):
            movie_name.append(word.lower())
            
        tweet_words = []
        for word in tweet.split(' '):
            if word.startswith('@') and len(word) > 1:
                word = "@user"
            elif word.startswith("http"):
                word = "http"
            elif word.startswith("bit.ly"):
                word = "http"
            elif str.lower(word) in movie_name:
                continue
            else:
                for item in movie_name:
                    if item in str.lower(word):
                        word = ""
            tweet_words.append(word)
        tweet_proc = " ".join(tweet_words)
        return tweet_proc
    
    def run_intense(self,intense_movie_list):
        sql_string = """
                SELECT movie_id, tweet.id, content, ai_rating, sanitized FROM tweet, movie
                WHERE movie_id in (%s)
                AND ai_rating IS NULL
                AND tweet.movie_id = movie.id
                """ % (', '.join(str(id) for id in intense_movie_list))
        df = pd.read_sql(sql_string,
                self.db.con)
        self.count = len(df.index)
        if self.count > 0 and not os.path.isfile(self.INTENSE_CSV_PATH):
            self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
            print("AI Sentiment Analysis: Using Device " + self.device)
            self.tokenizer = AutoTokenizer.from_pretrained('cardiffnlp/twitter-roberta-base-sentiment')
            self.model = AutoModelForSequenceClassification.from_pretrained('cardiffnlp/twitter-roberta-base-sentiment',from_tf=True).to(self.device)
            
            df['content'] = df.apply(lambda x: self.tweet_string(x['content'], x['sanitized']), axis = 1)
            
            df['ai_rating'] = df['content'].apply(lambda x: self.sentiment_score(x[:512]))
            
            df.to_csv(self.INTENSE_CSV_PATH, index= False)
            
            for row in df.itertuples():
                self.to_db(row.ai_rating, row.id)
            self.db.commit()
        elif self.count > 0:
            df_csv = pd.read_csv(self.INTENSE_CSV_PATH)
            for row in df_csv.itertuples():
                self.to_db(row.ai_rating, row.id)
            self.db.commit()
        else:
            print("TwitterSentimal: All intense tweets are already rated.")
    
    def run(self):
        df = pd.read_sql("""
                SELECT movie_id, tweet.id, content, ai_rating, sanitized FROM tweet, movie
                WHERE ai_rating IS NULL
                AND tweet.movie_id = movie.id
                """,
                self.db.con)
        self.count = len(df.index)
        if self.count > 0 and not os.path.isfile(self.CSV_PATH):
            self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
            print("AI Sentiment Analysis: Using Device " + self.device)
            self.tokenizer = AutoTokenizer.from_pretrained('cardiffnlp/twitter-roberta-base-sentiment')
            self.model = AutoModelForSequenceClassification.from_pretrained('cardiffnlp/twitter-roberta-base-sentiment',from_tf=True).to(self.device)
            
            df['content'] = df.apply(lambda x: self.tweet_string(x['content'], x['sanitized']), axis = 1)
            
            df['ai_rating'] = df['content'].apply(lambda x: self.sentiment_score(x[:512]))
            
            df.to_csv(self.CSV_PATH, index= False)
            
            for row in df.itertuples():
                self.to_db(row.ai_rating, row.id)
            self.db.commit()
        elif self.count > 0:
            df_csv = pd.read_csv(self.CSV_PATH)
            for row in df_csv.itertuples():
                self.to_db(row.ai_rating, row.id)
            self.db.commit()
        else:
            print("TwitterSentimal: All tweets are already rated.")