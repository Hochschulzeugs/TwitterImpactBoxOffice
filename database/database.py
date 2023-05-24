import sqlite3
import os

class Database:
    DBFILE = ""
    def __init__(self, datadir):
        Database.DBFILE = datadir + "/database.db"
        if not os.path.exists(datadir):
            os.mkdir(datadir)
        if os.path.isfile(Database.DBFILE):
            self.con = sqlite3.connect(Database.DBFILE)
            self.cur = self.con.cursor()
        else:
            print("Setting up SQLite database to {}".format(Database.DBFILE))
            self.con = sqlite3.connect(Database.DBFILE)
            self.cur = self.con.cursor()
            self.create()
        
    def __del__(self):
        self.con.close()
        
    def close(self):
        self.con.close()
        
    def create(self):
        print("Loading SQL scheme for database")
        f = open("database/Database.db.sql",'r')
        self.cur.executescript(f.read())
        self.commit()
        
    def get_movie_id(self, name):
        self.cur.execute("""
                       SELECT id FROM movie
                       WHERE name =?
                       """,
                       (name,)
                       )
        row = self.cur.fetchone()
        if row is not None:
            return row[0]
    
    def create_movie_list(self):
        self.cur.execute("""
            SELECT id, name, release, sanitized, rotten_link, rotten_genre FROM movie
            """
            )
        movie_list = self.cur.fetchall()
        if movie_list is not None:
            return movie_list
        
    def get_movie_id_sanitized(self, sanitized_name):
        self.cur.execute("""
                       SELECT id FROM movie
                       WHERE sanitized =?
                       """,
                       (sanitized_name,)
                       )
        row = self.cur.fetchone()
        if row is not None:
            return row[0]
        
    def commit(self):
        self.con.commit()
        