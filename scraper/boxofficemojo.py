import pandas as pd
import datetime
import time
import os

from dateutil.relativedelta import relativedelta
from re import sub
from decimal import Decimal
from database.database import Database
from pathvalidate import sanitize_filename

class Boxofficemojo:
    DATA_PATH = "/scrapedDataBoxOffice"
   
    def __init__(self, start_date, end_date, month_list, datadir):
        self.SCRAPED_DATA_PATH = datadir + self.DATA_PATH
        self.start_date = start_date
        self.end_date = end_date
        self.month_list = month_list
        self.formated_dates_list = []
        self.formated_dates_list_gross = []
        if not os.path.exists(self.SCRAPED_DATA_PATH):
            os.mkdir(self.SCRAPED_DATA_PATH)

    def create_time_range(self):
        date = self.start_date
        dates_list = []
        
        while date != self.end_date:
            month = date.strftime("%m")
            if month in self.month_list:
                dates_list.append(date)
            date += datetime.timedelta(1)
        return dates_list
    
    def create_time_range_gross(self):
        date = self.start_date
        end_date_gross = self.end_date + relativedelta(months=2)
        dates_list = []
        
        while date != end_date_gross:
            dates_list.append(date)
            date += datetime.timedelta(1)
        return dates_list

    def run(self):
        dates_list = self.create_time_range()
        self.formated_dates_list = [loop_date.strftime("%Y-%m-%d") for loop_date in dates_list]
        
        dates_list_gross = self.create_time_range_gross()
        self.formated_dates_list_gross = [loop_date.strftime("%Y-%m-%d") for loop_date in dates_list_gross]
        
        print("Start scraping gross data from boxofficemojo")

        dict_list = {}
        for item in self.formated_dates_list_gross:
            dict_list[item] = []

        for date,item in zip(self.formated_dates_list_gross, dict_list):
            if not os.path.isfile(self.SCRAPED_DATA_PATH + "/{}.csv".format(item)):
                url= "https://www.boxofficemojo.com/date/{}/?ref_=bo_da_nav".format(date)
                date = pd.read_html(url)
                time.sleep(10)
                dict_list[item].append(date[0])
                dict_list[item][0].to_csv(self.SCRAPED_DATA_PATH + "/{}.csv".format(item), index= False)
                print("boxoffice domestic gross {} is saved".format(item))
            else:
                print("Gross already existing, skipped {}".format(item))
                
    def to_database(self, db: Database):
        dir_path = os.fsencode(self.SCRAPED_DATA_PATH)
        for file in os.listdir(dir_path):
            filename = os.fsdecode(file)
            if filename.endswith(".csv"):
                date = datetime.datetime.strptime(filename.split('.')[0], "%Y-%m-%d")
                df = pd.read_csv(self.SCRAPED_DATA_PATH + "/" + filename)
                df.columns = df.columns.str.replace(' ','_')
                for row in df.itertuples():
                    movie_id = db.get_movie_id(row.Release)
                    if date.strftime("%Y-%m-%d") in self.formated_dates_list and row.New_This_Day:
                        if movie_id == None and "release" not in row.Release and "Release" not in row.Release:
                            db.cur.execute("""
                                INSERT INTO movie (name, release, distributor, sanitized)
                                VALUES (?,?,?,?)
                                """,
                                (row.Release,
                                filename.split('.')[0],
                                row.Distributor,
                                sanitize_filename(row.Release),)
                                )
                            movie_id = db.get_movie_id(row.Release)
                            print("New movie saved to Database: {}".format(row.Release))
                    if movie_id != None:       
                        db.cur.execute("""
                                SELECT date, movie_id FROM gross
                                WHERE date =?
                                AND movie_id =?
                                """,
                                (filename.split('.')[0],
                                    movie_id,)
                                )
                        sqlrow = db.cur.fetchone()
                        if sqlrow == None:
                            money = Decimal(sub(r'[^\d\-.]', '', row.Daily))
                            db.cur.execute("""
                                INSERT INTO gross (date, movie_id, gross)
                                VALUES (?,?,?)
                                """,
                                (filename.split('.')[0],
                                movie_id,
                                float(money),)
                                )
                            print("New Gross Data for movie: {}".format(row.Release))
        db.commit()