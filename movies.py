import csv
import pymysql
import pandas
import sys

username = sys.argv[1]
password = sys.argv[2]

# globals for the connection, cursor and csv file to be used later
conn = pymysql.connect(host='localhost', port=3306, user=username, passwd=password, db='assignment5')
cur = conn.cursor()
file = "tmdb_5000_movies.csv"


# SQL queries, prints out the requested query or all 5 if not specified
def sqlqueries(query):
    num = int(query)
    if (num == 1) or (num == 0):
        sql = "SELECT AVG(budget) FROM movie"
        cur.execute(sql)
        conn.commit()
        print("%s" % cur.fetchone())
    if num == 2 or num == 0:
        sql = "SELECT title, company_name FROM movie " \
              "INNER JOIN movie_prod_countries ON movie.id = movie_prod_countries.movie_id " \
              "INNER JOIN movie_production_company ON movie.id = movie_production_company.movie_id " \
              "INNER JOIN production_companies ON movie_production_company.prod_id = production_companies.company_id " \
              "WHERE iso_3166 = 'US' LIMIT 5"
        cur.execute(sql)
        conn.commit()
        rows = cur.fetchall()
        for row in rows:
            print("Title: %s |" % row[0] + " Production Company: %s" % row[1])
    if num == 3 or num == 0:
        sql = "SELECT title, revenue FROM movie ORDER BY revenue DESC LIMIT 5"
        cur.execute(sql)
        conn.commit()
        rows = cur.fetchall()
        print('------------------------------------')
        print('| {:^15} | {:^15}'.format('Title', 'Revenue'), end='|')
        print('\n------------------------------------')
        for row in rows:
            print('| {:^15} | {:^15}'.format(row[0], row[1]), end='|')
            print()
        print('------------------------------------\n')
#            print("Title: %s |" % row[0] + " Revenue: %s" % row[1])
    if num == 4 or num == 0:
        sql = "SELECT title, genre.genre_name " \
              "FROM ( " \
                "SELECT movie.title, genre.genre_name, movie.id " \
                "FROM movie " \
                "INNER JOIN movie_genres ON movie.id=movie_genres.movie_id " \
                "INNER JOIN genre ON movie_genres.genre_id=genre.id " \
                "WHERE genre.genre_name IN ('Science Fiction', 'Mystery') " \
                "GROUP BY movie.id HAVING COUNT(movie.id) > 1 " \
              ") as t1 " \
              "INNER JOIN movie_genres ON t1.id=movie_genres.movie_id " \
              "INNER JOIN genre ON movie_genres.genre_id=genre.id LIMIT 5;"
        cur.execute(sql)
        conn.commit()
        rows = cur.fetchall()
        print('-----------------------------------------------------')
        print('| {:^30} | {:^17}'.format('Title', 'Genre Name'), end='|')
        print('\n-----------------------------------------------------')
        for row in rows:
            print('| {:^30} | {:^17}'.format(row[0], row[1]), end='|')
            print()
        print('-----------------------------------------------------\n')
    if num == 5 or num == 0:
        sql = "SELECT title, popularity " \
              "FROM movie " \
              "WHERE popularity > (SELECT AVG(popularity) FROM movie) LIMIT 5"
        cur.execute(sql)
        conn.commit()
        rows = cur.fetchall()
        print('------------------------------------')
        print('| {:^15} | {:^15}'.format('Title', 'Popularity'), end='|')
        print('\n------------------------------------')
        for row in rows:
            print('| {:^15} | {:^15}'.format(row[0], row[1]), end='|')
            print()
        print('------------------------------------\n')


# creates all the tables in the database
def createtables():
    with conn.cursor() as cur:
        sql = "CREATE TABLE movie (id INT, title VARCHAR(255), homepage VARCHAR(500), overview VARCHAR(1000), tagline VARCHAR(500), runtime INT, release_date DATE," \
              " status VARCHAR(20), original_language VARCHAR(2), budget INT, revenue BIGINT," \
              " original_title VARCHAR(255), popularity DOUBLE PRECISION(10, 6), vote_average DEC(3,1), vote_count INT," \
              " PRIMARY KEY(id));"
        cur.execute(sql)
        sql = "CREATE TABLE genre (id INT, genre_name VARCHAR(50), PRIMARY KEY(id));"
        cur.execute(sql)
        sql = "CREATE TABLE movie_genres (movie_id INT, genre_id INT, PRIMARY KEY(movie_id, genre_id)," \
              " FOREIGN KEY(movie_id) REFERENCES assignment5.movie(id) ON UPDATE CASCADE ON DELETE CASCADE, " \
              "FOREIGN KEY(genre_id) REFERENCES assignment5.genre(id) ON UPDATE CASCADE ON DELETE CASCADE);"
        cur.execute(sql)
        sql = "CREATE TABLE production_companies(company_id INT, company_name VARCHAR(255), PRIMARY KEY(company_id));"
        cur.execute(sql)
        sql = "CREATE TABLE movie_production_company (movie_id INT, prod_id INT, PRIMARY KEY(movie_id, prod_id), " \
              "FOREIGN KEY(movie_id) REFERENCES assignment5.movie(id) ON UPDATE CASCADE ON DELETE CASCADE," \
              "FOREIGN KEY(prod_id) REFERENCES assignment5.production_companies(company_id) ON UPDATE CASCADE ON DELETE CASCADE);"
        cur.execute(sql)
        sql = "CREATE TABLE keywords(id INT, keyword_name VARCHAR(100), PRIMARY KEY(id));"
        cur.execute(sql)
        sql = "CREATE TABLE movie_keywords (movie_id INT, key_id INT, PRIMARY KEY(movie_id, key_id), " \
              "FOREIGN KEY(movie_id) REFERENCES assignment5.movie(id) ON UPDATE CASCADE ON DELETE CASCADE, " \
              "FOREIGN KEY(key_id) REFERENCES assignment5.keywords(id) ON UPDATE CASCADE ON DELETE CASCADE);"
        cur.execute(sql)
        sql = "CREATE TABLE production_countries(iso_3166_1 VARCHAR(2), country VARCHAR(50), PRIMARY KEY(iso_3166_1));"
        cur.execute(sql)
        sql = "CREATE TABLE movie_prod_countries(movie_id INT, iso_3166 VARCHAR(2), PRIMARY KEY(movie_id, iso_3166), " \
              "FOREIGN KEY(movie_id) REFERENCES assignment5.movie(id) ON UPDATE CASCADE ON DELETE CASCADE, " \
              "FOREIGN KEY(iso_3166) REFERENCES assignment5.production_countries(iso_3166_1) ON UPDATE CASCADE ON DELETE CASCADE);"
        cur.execute(sql)
        sql = "CREATE TABLE spoken_languages(iso_639_1 VARCHAR(2), language_name VARCHAR(25), PRIMARY KEY(iso_639_1));"
        cur.execute(sql)
        sql = "CREATE TABLE movie_languages(movie_id INT, iso_639 VARCHAR(2), PRIMARY KEY(movie_id, iso_639), " \
              "FOREIGN KEY(movie_id) REFERENCES assignment5.movie(id) ON UPDATE CASCADE ON DELETE CASCADE, " \
              "FOREIGN KEY(iso_639) REFERENCES assignment5.spoken_languages(iso_639_1) ON UPDATE CASCADE ON DELETE CASCADE);"
        cur.execute(sql)

    conn.commit()


# inserts all the unique keyword pairs
def insertkey(keys):
        sql = "INSERT INTO keywords(id, keyword_name) VALUES(%s, %s)"
        cur.execute(sql, (keys[0], keys[1]))


# inserts into the keyword relation table (the logic is the same for all relation tables)
def insertmoviekey(movieID, keyID):
    for pair in keyID.itertuples(index=False):  # iterate through the tuples, inserting each one into the relation table
        sql = "INSERT INTO movie_keywords(movie_id, key_id) VALUES(%s, %s)"
        cur.execute(sql, (movieID, pair[0]))


# inserts all the unique genre pairs
def insertgenres(uniquegenres):
    sql = "INSERT INTO genre (id, genre_name) VALUES(%s, %s)"
    cur.execute(sql, (uniquegenres[0], uniquegenres[1]))


# inserts into the genre relation table
def insertmoviegenres(movieID, genreID):
        for pair in genreID.itertuples(index=False):
            sql = "INSERT INTO movie_genres(movie_id, genre_id) VALUES(%s, %s)"
            cur.execute(sql, (movieID, pair[0]))


# inserts all the unique company pairs
def insertcompanies(companies):
        sql = "INSERT INTO production_companies(company_id, company_name) VALUES(%s, %s)"
        cur.execute(sql, (companies[0], companies[1]))


# inserts into the companies relation table
def moviecompany(movieID, compID):
        for pair in compID.itertuples(index=False):
            sql = "INSERT INTO movie_production_company(movie_id, prod_id) VALUES(%s, %s)"
            cur.execute(sql, (movieID, pair[0]))


# inserts all the unique country pairs
def insertcountires(country):
        sql = "INSERT INTO production_countries(iso_3166_1, country) VALUES(%s, %s)"
        cur.execute(sql, (country[0], country[1]))


# inserts into the country relation table
def moviecountries(movieID, countryISO):
        for pair in countryISO.itertuples(index=False):
            sql = "INSERT INTO movie_prod_countries(movie_id, iso_3166) VALUES(%s, %s)"
            cur.execute(sql, (movieID, pair[0]))


# inserts all the unique language pairs
def insertlanguage(languages):
        sql = "INSERT INTO spoken_languages(iso_639_1, language_name) VALUES(%s, %s)"
        cur.execute(sql, (languages[0], languages[1]))


#insert into the language relation table
def movielanguages(movieID, langID):
        for pair in langID.itertuples(index=False):
            sql = "INSERT INTO movie_languages(movie_id, iso_639) VALUES(%s, %s)"
            cur.execute(sql, (movieID, pair[0]))


# function to insert the data into the movie table
def insertmovie(data):
        if data['release_date'] == '':  # set the date to NULL if there is no release date attribute
            date = None
        else:   # put the date into the correct format for an SQL date type
            date = data['release_date'].replace('/', '-')
        runtime = data['runtime']
        if data['runtime'] == '':   # if the runtime is the empty string set it equal to 0
            runtime = 0
        sql = "INSERT INTO movie(id, title, homepage, overview, tagline, runtime, release_date, status, original_language, budget, revenue, original_title, popularity, vote_average, vote_count) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cur.execute(sql, (data['id'], data['title'], data['homepage'], data['overview'], data['tagline'], runtime, date, data['status'], data['original_language'], data['budget'], data['revenue'], data['original_title'], data['popularity'], data['vote_average'], data['vote_count']))


createtables()
with open(file, encoding="utf8") as csvfile:
    # the lists hold all the unique pairs found while reading the file
    uniquegenres = []
    uniquekey = []
    uniquecompanies = []
    uniquecountries = []
    uniquelanguages = []
    csvreader = csv.DictReader(csvfile)
    for row in csvreader:  # iterate through each row in the csv file
        insertmovie(row)    # insert all the data from the current row into the movie table
        for attribute in row:  # iterate through each column in the current row
            # only parse the columns that are in JSON
            if (attribute == 'genres') or (attribute == 'keywords') or (attribute == 'production_companies') or (attribute == 'production_countries') or (attribute == 'spoken_languages'):
                # the logic for all the elif statements is the same so I'm only going to explain it once
                if attribute == 'genres':
                    genre = pandas.read_json(row[attribute])    # read the column thats in JSON into a pandas dataframe
                    for pair in genre.itertuples(index=False):  # iterate through each row in the dataframe
                        if tuple(pair) not in uniquegenres:     # if the tuple is not already in the list holding the unique pairs
                            uniquegenres.append(tuple(pair))    # add the tuple not present to the list
                            insertgenres(tuple(pair))           # insert the tuple into the genre table
                    insertmoviegenres(row['id'], genre)         # finally insert all the pairs in the data frame into the relation table
                elif attribute == 'keywords':
                    keyword = pandas.read_json(row[attribute])
                    for pair in keyword.itertuples(index=False):
                        if tuple(pair) not in uniquekey:
                            uniquekey.append(tuple(pair))
                            insertkey(tuple(pair))
                    insertmoviekey(row['id'], keyword)
                elif attribute == 'production_companies':
                    companies = pandas.read_json(row[attribute])
                    for pair in companies.itertuples(index=False):
                        if tuple(pair) not in uniquecompanies:
                            uniquecompanies.append(tuple(pair))
                            insertcompanies(tuple(pair))
                    moviecompany(row['id'], companies)
                elif attribute == 'production_countries':
                    countries = pandas.read_json(row[attribute])
                    for pair in countries.itertuples(index=False):
                        if tuple(pair) not in uniquecountries:
                            uniquecountries.append(tuple(pair))
                            insertcountires(tuple(pair))
                    moviecountries(row['id'], countries)
                elif attribute == 'spoken_languages':
                    languages = pandas.read_json(row[attribute])
                    for pair in languages.itertuples(index=False):
                        if tuple(pair) not in uniquelanguages:
                            uniquelanguages.append(tuple(pair))
                            insertlanguage(tuple(pair))
                    movielanguages(row['id'], languages)
    conn.commit()  # commit all the inserts
    # after inserting everything do the SQL queries
    if len(sys.argv) != 4:  # if there no query arg do all the queries
        sqlqueries(0)
    elif (int(sys.argv[3]) >= 1) and (int(sys.argv[3]) <= 5):
        sqlqueries(sys.argv[3])
