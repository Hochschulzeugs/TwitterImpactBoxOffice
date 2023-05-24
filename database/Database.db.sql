BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "gross" (
	"date"	TEXT NOT NULL,
	"movie_id"	INTEGER,
	"gross"	REAL,
	PRIMARY KEY("date","movie_id"),
	FOREIGN KEY("movie_id") REFERENCES "movie"("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "rotten_tomatoes_reviews" (
	"id"	INTEGER NOT NULL UNIQUE,
	"movie_id"	INTEGER NOT NULL,
	"critic_name"	TEXT,
	"publisher_name"	TEXT,
	"is_top_critic"	INTEGER,
	"review_date"	TEXT,
	"is_good_review"	INTEGER NOT NULL,
	"review_content"	TEXT,
	PRIMARY KEY("id" AUTOINCREMENT),
	FOREIGN KEY("movie_id") REFERENCES "movie"("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "tweet" (
	"id"	INTEGER NOT NULL,
	"movie_id"	INTEGER NOT NULL,
	"date"	TEXT,
	"likeCount"	INTEGER,
	"retweetcount"	INTEGER,
	"replies"	INTEGER,
	"quoteCount"	INTEGER,
	"sourceLabel"	TEXT,
	"username"	TEXT,
	"content"	TEXT,
	"ai_rating"	INTEGER,
	PRIMARY KEY("id" AUTOINCREMENT),
	FOREIGN KEY("movie_id") REFERENCES "movie"("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "movie" (
	"id"	INTEGER UNIQUE,
	"name"	TEXT NOT NULL UNIQUE,
	"release"	TEXT,
	"distributor"	TEXT,
	"sanitized"	TEXT,
	"rotten_tomatoe_rating"	INTEGER,
	"rotten_tomatoe_count"	INTEGER,
	"rotten_audience_rating"	INTEGER,
	"rotten_audience_count"	INTEGER,
	"rotten_genre"	TEXT,
	"rotten_link"	TEXT UNIQUE,
	PRIMARY KEY("id" AUTOINCREMENT)
);
COMMIT;
