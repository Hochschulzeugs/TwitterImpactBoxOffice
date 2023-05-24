import os
import datetime
import pandas as pd
import re

from database.database import Database
from dateutil.relativedelta import relativedelta
from difflib import SequenceMatcher

class Rotten:    
    def __init__(self, db: Database, kaggle_user, kaggle_api_key, datadir):
        self.SCRAPED_DATA_PATH = datadir
        if not os.path.exists(self.SCRAPED_DATA_PATH):
            os.mkdir(self.SCRAPED_DATA_PATH)
        if not os.path.isfile(self.SCRAPED_DATA_PATH + '/rotten_tomatoes_movies.csv') or  not os.path.isfile(self.SCRAPED_DATA_PATH + '/rotten_tomatoes_critic_reviews.csv'):
            os.environ['KAGGLE_USERNAME'] = kaggle_user
            os.environ['KAGGLE_KEY'] = kaggle_api_key
            from kaggle.api.kaggle_api_extended import KaggleApi
            kapi = KaggleApi()
            kapi.authenticate()
            kapi.dataset_download_files('stefanoleone992/rotten-tomatoes-movies-and-critic-reviews-dataset', path="data/", unzip=True)
        self.db = db
    
    def genre_to_database(self):
        file_path_movies = self.SCRAPED_DATA_PATH + "/rotten_tomatoes_movies.csv"
        df_movies = pd.read_csv(file_path_movies)
        movie_list = self.db.create_movie_list()
        for movie in movie_list:
            try:
                rotten_link = movie[4]
                rotten_genres = movie[5]
            except:
                ""
            if rotten_link is None or rotten_genres is not None:
                continue
            row = df_movies.loc[df_movies['rotten_tomatoes_link'] == rotten_link]
            if row is not None and row.genres is not None:
                genre = row['genres'].to_string(index=False, header=False)
                self.db.cur.execute("""
                                UPDATE movie
                                SET rotten_genre =?
                                WHERE id =?
                                """,
                                (genre,
                                movie[0],)
                                )
        self.db.commit()
                
                
        
    def moviedata_to_database(self):
        file_path_movies = self.SCRAPED_DATA_PATH + "/rotten_tomatoes_movies.csv"
        df_movies = pd.read_csv(file_path_movies)
        movie_list = self.db.create_movie_list()
        
        margin = relativedelta(years = 2)
        for movie in movie_list:
            if movie[4] is not None:
                continue
            found = False
            db_date = datetime.datetime.strptime(str(movie[2]), "%Y-%m-%d")
            for row in df_movies.itertuples():
                try:
                    rotten_date = datetime.datetime.strptime(str(row.original_release_date), "%Y-%m-%d")
                except:
                    ""
                if rotten_date is not None:
                    
                    if str.lower(re.sub(r"[^a-zA-Z0-9 ]", "", row.movie_title)) == str.lower(re.sub(r"[^a-zA-Z0-9 ]", "", movie[1])):
                        if rotten_date - margin <= db_date <= rotten_date + margin:
                            self.db.cur.execute("""
                                UPDATE movie
                                SET rotten_tomatoe_rating =?,
                                    rotten_tomatoe_count =?,
                                    rotten_audience_rating =?,
                                    rotten_audience_count =?,
                                    rotten_link =?
                                WHERE id =?
                                """,
                                (row.tomatometer_rating,
                                row.tomatometer_count,
                                row.audience_rating,
                                row.audience_count,
                                row.rotten_tomatoes_link,
                                movie[0],)
                                )
                            found = True
                            break
                        self.db.commit()
            if not found:
                for row in df_movies.itertuples():
                    try:
                        rotten_date = datetime.datetime.strptime(str(row.original_release_date), "%Y-%m-%d")
                    except:
                        ""
                    if rotten_date is not None:
                        if SequenceMatcher(None, str.lower(re.sub(r"[^a-zA-Z0-9 ]", "", row.movie_title)), str.lower(re.sub(r"[^a-zA-Z0-9 ]", "", movie[1]))).ratio() > 0.7:
                            if rotten_date - relativedelta(years = 1) <= db_date <= rotten_date + relativedelta(years = 1):
                                print("\nMaybe found title:\n" + movie[1] + " " + db_date.strftime("%Y-%m-%d") + " (DB)\n" + row.movie_title + " " + rotten_date.strftime("%Y-%m-%d") +" (rotten)")
                                answer = "c"
                                while True:
                                    answer = input("Store Movie in DB? (y/n):\n")
                                    if answer == "y":
                                        self.db.cur.execute("""
                                            UPDATE movie
                                            SET rotten_tomatoe_rating =?,
                                                rotten_tomatoe_count =?,
                                                rotten_audience_rating =?,
                                                rotten_audience_count =?,
                                                rotten_link =?
                                            WHERE id =?
                                            """,
                                            (row.tomatometer_rating,
                                            row.tomatometer_count,
                                            row.audience_rating,
                                            row.audience_count,
                                            row.rotten_tomatoes_link,
                                            movie[0],)
                                            )
                                        print("Saved to DB!")
                                        found = True
                                        break
                                    elif answer == "n":
                                        print("NOT saved to DB!")
                                        break
                                self.db.commit()
                                    
    def critic_to_database(self):
        file_path_critics = self.SCRAPED_DATA_PATH + "/rotten_tomatoes_critic_reviews.csv"
        df_critics = pd.read_csv(file_path_critics)
        movie_list = self.db.create_movie_list()
        
        for movie in movie_list:
            self.db.cur.execute("""
                SELECT movie_id FROM rotten_tomatoes_reviews
                WHERE movie_id =?
                """,
                (movie[0],)
                )
            sqlrow = self.db.cur.fetchone()
            if sqlrow == None:
                for row in df_critics.itertuples():
                    if row.rotten_tomatoes_link == movie[4]:
                        review_type = False
                        if row.review_type == "Fresh":
                            review_type = True
                        self.db.cur.execute("""
                            INSERT INTO rotten_tomatoes_reviews (movie_id, critic_name, publisher_name, is_top_critic, review_date, is_good_review, review_content)
                            VALUES (?,?,?,?,?,?,?)
                            """,
                            (movie[0],
                            row.critic_name,
                            row.publisher_name,
                            row.top_critic,
                            row.review_date,
                            review_type,
                            row.review_content,)
                            )
                self.db.commit()
