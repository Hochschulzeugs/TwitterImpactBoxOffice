import os
import datetime
import snscrape.modules.twitter as sntwitter
import pandas as pd

from database.database import Database
from dateutil.relativedelta import relativedelta
from pathvalidate import sanitize_filename


class Twitter:
    DATA_PATH = "/scrapedDataTwitter"
    
    def __init__(self, db: Database, datadir):
        self.SCRAPED_DATA_PATH = datadir + self.DATA_PATH
        if not os.path.exists(self.SCRAPED_DATA_PATH):
            os.mkdir(self.SCRAPED_DATA_PATH)
        self.db = db
            
    def run(self):
        movie_list = self.db.create_movie_list()
        print("Start scraping tweets from Twitter for {} movies".format(len(movie_list)))
        for movie in movie_list:
            if not os.path.isfile(self.SCRAPED_DATA_PATH + "/{}-{}.csv".format(movie[0], sanitize_filename(movie[1]))):
                release = datetime.datetime.strptime(movie[2], "%Y-%m-%d")
                two_month_before_release = release - relativedelta(months=2)
                two_month_after_release = release + relativedelta(months=2)
                #"ready player one movie lang:en until:2018-05-29 since:2018-01-29"
                query = sanitize_filename(movie[1]) + " movie " + "lang:en until:" + two_month_after_release.strftime("%Y-%m-%d") + " since:" + two_month_before_release.strftime("%Y-%m-%d")
                tweets = []
                print("Start scraping from twitter for movie {}".format(movie[1]))
                for i,tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
                    if i>5000:
                        break
                    tweets.append([tweet.date, tweet.likeCount, tweet.retweetCount, tweet.replyCount, tweet.quoteCount, tweet.sourceLabel, tweet.user.username, tweet.rawContent])
                tweets_df = pd.DataFrame(tweets, columns=['date', 'likeCount', 'retweetcount', 'replies', 'quoteCount', 'sourceLabel', 'username', 'content'])
                tweets_df.to_csv(self.SCRAPED_DATA_PATH + "/{}-{}.csv".format(movie[0], sanitize_filename(movie[1])), index= False)
                
                
                
            else:
                print("Data for movie {} already exists".format(movie[1]))
                
    def to_database(self):
        dir_path = os.fsencode(self.SCRAPED_DATA_PATH)
        for file in os.listdir(dir_path):
            filename = os.fsdecode(file)
            if filename.endswith(".csv"):
                movie = os.path.splitext(filename.split("-", 1)[1])[0]
                movie_id = self.db.get_movie_id_sanitized(movie)
                
                if movie_id != None:       
                    self.db.cur.execute("""
                            SELECT movie_id FROM tweet
                            WHERE movie_id =?
                            """,
                            (movie_id,)
                            )
                    sqlrow = self.db.cur.fetchone()
                    if sqlrow == None:
                        df = pd.read_csv(self.SCRAPED_DATA_PATH + "/" + filename)
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
                        print("Tweets already in DB for Movie {}".format(movie))