import os
import datetime
import snscrape.modules.twitter as sntwitter
import pandas as pd

from database.database import Database
from dateutil.relativedelta import relativedelta

class IntenseTwitter:
    DATA_PATH = "/scrapedDataIntenseTwitter"
    def __init__(self, db: Database, movie_list, datadir):
        self.SCRAPED_DATA_PATH = datadir + self.DATA_PATH
        if not os.path.exists(self.SCRAPED_DATA_PATH):
            os.mkdir(self.SCRAPED_DATA_PATH)
        self.movie_list = movie_list
        self.db = db
    
    def run(self):
        movie_list = self.movie_list
        for movie in movie_list:
            self.db.cur.execute("""
                    SELECT * FROM movie
                    WHERE id =?
                    """,
                    (movie,)
                    )
            sqlrow = self.db.cur.fetchone()
            if sqlrow != None and not os.path.isfile(self.SCRAPED_DATA_PATH + "/{}-{}.csv".format(movie, sqlrow[4])):
                release = datetime.datetime.strptime(sqlrow[2], "%Y-%m-%d")
                two_month_before_release = release - relativedelta(months=2)
                two_month_after_release = release + relativedelta(months=2)
                #"ready player one movie lang:en until:2018-05-29 since:2018-01-29"
                if movie == 364:
                    query = "Star Wars Rise of Skywalker " + "lang:en until:" + two_month_after_release.strftime("%Y-%m-%d") + " since:" + two_month_before_release.strftime("%Y-%m-%d")
                elif movie == 292:
                    query = "Us movie Peele " + "lang:en until:" + two_month_after_release.strftime("%Y-%m-%d") + " since:" + two_month_before_release.strftime("%Y-%m-%d")
                else:
                    query = sqlrow[4] + " movie " + "lang:en until:" + two_month_after_release.strftime("%Y-%m-%d") + " since:" + two_month_before_release.strftime("%Y-%m-%d")
                tweets = []
                print("Start intense scraping from twitter for movie {}".format(sqlrow[4]))
                for tweet in sntwitter.TwitterSearchScraper(query).get_items():
                    tweets.append([tweet.date, tweet.likeCount, tweet.retweetCount, tweet.replyCount, tweet.quoteCount, tweet.sourceLabel, tweet.user.username, tweet.rawContent])
                tweets_df = pd.DataFrame(tweets, columns=['date', 'likeCount', 'retweetcount', 'replies', 'quoteCount', 'sourceLabel', 'username', 'content'])
                tweets_df.to_csv(self.SCRAPED_DATA_PATH + "/{}-{}.csv".format(movie, sqlrow[4]), index= False)
            else:
                print("Movie with id {} not found".format(movie))
                
                
    def to_database(self):
        dir_path = os.fsencode(self.SCRAPED_DATA_PATH)
        for file in os.listdir(dir_path):
            filename = os.fsdecode(file)
            if filename.endswith(".csv"):
                movie = os.path.splitext(filename.split("-", 1)[1])[0]
                movie_id = self.db.get_movie_id_sanitized(movie)
                
                if movie_id != None:       
                    df = pd.read_csv(self.SCRAPED_DATA_PATH + "/" + filename)
                    self.db.cur.execute("""
                            DELETE FROM tweet
                            WHERE movie_id =?
                            """,
                            (movie_id,)
                            )
                    print("Adding Tweets in DB for Movie {}".format(movie))
                    for row in df.itertuples():
                        self.db.cur.execute("""
                                INSERT INTO tweet (movie_id, date, likeCount, retweetcount, replies, quoteCount, sourceLabel, username, content)
                                VALUES (?,?,?,?,?,?,?,?,?)
                                """,
                                (movie_id,
                                row.date,
                                row.likeCount,
                                row.retweetcount,
                                row.replies,
                                row.quoteCount,
                                row.sourceLabel,
                                row.username,
                                row.content,)
                                )
                    self.db.commit()
                else:
                    print("Movie {} not found".format(movie))       