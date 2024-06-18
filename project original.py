import pandas as pd
import numpy as np
from db_conn import *

def read_excel_into_mysql():
    try:
        excel_file = "movie_list.xls"
        print("Loading Excel file...")

        df_tab1 = pd.read_excel(excel_file, sheet_name='movie1', skiprows=4)
        df_tab2 = pd.read_excel(excel_file, sheet_name='movie2', header=None)
        print("Opening database connection...")
        conn, cur = open_db()
        
        df_tab1 = df_tab1.replace({np.nan: None})
        df_tab2 = df_tab2.replace({np.nan: None})
        dfs = [df_tab1, df_tab2]
        print("Concatenating data from both sheets...")

        movie_table = "movieDB.Movies"
        genre_table = "movieDB.Genres"
        director_table = "movieDB.Directors"
        moviedirector_table = "movieDB.Movie_Director"
        
        create_sql = f"""
            SET foreign_key_checks = 0;

            DROP TABLE IF EXISTS {movie_table};
            DROP TABLE IF EXISTS {genre_table};
            DROP TABLE IF EXISTS {director_table};
            DROP TABLE IF EXISTS {moviedirector_table};
            SET foreign_key_checks = 1;

            CREATE TABLE {movie_table} (
                movie_id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(500),
                eng_title VARCHAR(500),
                year INT,
                country VARCHAR(100),
                m_type VARCHAR(10),
                status VARCHAR(30),
                company VARCHAR(255)
            );
            CREATE TABLE {genre_table}(
                movie_id INT,
                genre_name VARCHAR(255),
                FOREIGN KEY (movie_id) REFERENCES {movie_table}(movie_id) ON DELETE CASCADE
            );
            CREATE TABLE {director_table}(
                director_id INT AUTO_INCREMENT PRIMARY KEY,
                director_name VARCHAR(255)
            );
            CREATE TABLE {moviedirector_table}(
                movie_id INT,
                director_id INT,
                FOREIGN KEY (movie_id) REFERENCES {movie_table}(movie_id) ON DELETE CASCADE,
                FOREIGN KEY (director_id) REFERENCES {director_table}(director_id) ON DELETE CASCADE
            );
            CREATE INDEX idx_movie_id ON {genre_table}(movie_id);
            CREATE INDEX idx_year ON {movie_table}(year);
            CREATE FULLTEXT INDEX idx_title ON {movie_table}(title);
            CREATE FULLTEXT INDEX idx_name ON {director_table}(director_name);
        """
        print("Creating tables...")
        cur.execute(create_sql)
        conn.commit()

        movie_insert = f"""INSERT INTO {movie_table} (title, eng_title, year, country, m_type, status, company)
                           VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        director_insert = f"""INSERT INTO {director_table} (director_name) VALUES (%s)
                              ON DUPLICATE KEY UPDATE director_name=director_name;"""
        genre_insert = f"""INSERT INTO {genre_table} (movie_id, genre_name) VALUES (%s, %s);"""
        moviedirector_insert = f"""INSERT INTO {moviedirector_table} (movie_id, director_id) VALUES (%s, %s);"""
        select_last_movie_id_sql = f"""SELECT MAX(movie_id) AS id FROM {movie_table};"""
        select_last_director_id_sql = f"""SELECT MAX(director_id) AS id FROM {director_table};"""
        
        movies_data = []
        directors_data = []
        genres_data = []

        print("Processing rows...")
        for df in dfs:
            for i, r in df.iterrows():
                row = tuple(r)
                title = row[0]
                eng_title = row[1]
                year = row[2]
                country = row[3]
                m_type = row[4]
                genre = row[5]
                status = row[6]
                director = row[7]
                company = row[8]

                # Insert movie data
                movies_data.append((title, eng_title, year, country, m_type, status, company))
                directors_data.append(director)
                genres_data.append(genre)

        try:
            print("Inserting movies...")
            cur.executemany(movie_insert, movies_data)
            conn.commit()

            cur.execute(select_last_movie_id_sql)
            last_movie_id = cur.fetchone()["id"]
            first_movie_id = last_movie_id - len(movies_data) + 1

            for movie_id in range(first_movie_id, last_movie_id + 1):
                row_index = movie_id - first_movie_id
                if genres_data[row_index]:
                    genres_data[row_index] = (movie_id, genres_data[row_index])
                else:
                    genres_data[row_index] = None
            genres_data = [x for x in genres_data if x is not None]
            
            print("Inserting genres...")
            if genres_data:
                cur.executemany(genre_insert, genres_data)
                conn.commit()
            else:
                print("No genre data to insert.")

            print("Inserting directors...")
            unique_directors = set()
            for directors in directors_data:
                if directors:
                    for director in directors.split(","):
                        unique_directors.add(director.strip())
            unique_directors = list(unique_directors)
            cur.executemany(director_insert, [(director,) for director in unique_directors])
            conn.commit()
            print("Director insert complete")

            print("Inserting movie-director relations...")
            movie_director_data = []
            for movie_id in range(first_movie_id, last_movie_id + 1):
                row_index = movie_id - first_movie_id
                if directors_data[row_index]:
                    movie_director_data.append((movie_id, directors_data[row_index]))
            cur.executemany(moviedirector_insert, movie_director_data)
            conn.commit()
            print("Movie-director relations insert complete")

        except Exception as e:
            print(f"Error during data insertion: {e}")
            conn.rollback()

        close_db(conn, cur)
        print("Done.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    read_excel_into_mysql()
