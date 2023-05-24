import datetime
import os
from database.database import Database
from scraper.boxofficemojo import Boxofficemojo
from scraper.twitter import Twitter
from rottenTomatoes.rotten import Rotten
from sentimentAnalysis.twitsent import TwitterSentimal
from scraper.intensetwitter import IntenseTwitter

#Data DIR for csv output and database
datadir = "data"

#Database setup
db = Database(datadir)

##Scrape Box Office Data for given date-range and months
start_date = datetime.date(2018, 1, 1)
end_date = datetime.date(2019, 12, 31) #Gross-data will be colected until two months after end_date, but no new movies will be stored in db
month_list = ["01", "02", "03", "11", "12"]

bom = Boxofficemojo(start_date, end_date, month_list, datadir)
bom.run()
bom.to_database(db)

##Scrape Twitter Data for all movies in db with 5k-tweet-limit
twit = Twitter(db, datadir)
twit.run()
twit.to_database()

##Rotten Tomatoes - Extract from dataset https://www.kaggle.com/datasets/stefanoleone992/rotten-tomatoes-movies-and-critic-reviews-dataset
rotten = Rotten(db, kaggle_user='your_username', kaggle_api_key='your_api_key', datadir=datadir) #Create free API Key from Kaggle Account-Page
rotten.moviedata_to_database()
rotten.critic_to_database()
rotten.genre_to_database()


##Tweet Sentiment Analysis for all tweets in db
ts = TwitterSentimal(db, datadir)
ts.run()


##Intense Tweet scraping - Removing and re-scrapping all tweets for list of movie-ids (no Limit)
movie_id_list = [364,86,160,56,358,126,292,269,36,343,369,122,130,98,226,216,296,200,81,123] #List of IDs for intense Twitter-scrapint and new sentiment analysis
itwit = IntenseTwitter(db,movie_id_list,datadir)
itwit.run()
itwit.to_database()
ts.run_intense(movie_id_list)