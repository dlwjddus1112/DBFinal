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
        df_tab2 = df_tab2.replace({np.nn: None})
        dfs = [df_tab1,df_tab2]
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
                genre VARCHAR(100),
                status VARCHAR(30),
                director VARCHAR(255),
                company VARCHAR(255),
                enter_date DATETIME DEFAULT NOW()
            );
            CREATE TABLE {genre_table}(
                movie_id INT,
                genre_name VARCHAR(255) NOT NULL,
                FOREIGN KEY (movie_id) REFERENCES {movie_table}(movie_id) ON DELETE CASCADE
            );
            CREATE TABLE {director_table}(
                director_id INT AUTO_INCREMENT PRIMARY KEY,
                director_name VARCHAR(255) NOT NULL
               
            );
            CREATE TABLE {moviedirector_table}(
                movie_id INT,
                director_id INT,
                FOREIGN KEY (movie_id) REFERENCES {movie_table}(movie_id) ON DELETE CASCADE,
                FOREIGN KEY (director_id) REFERENCES {director_table}(director_id) ON DELETE CASCADE
            );
             create index idx_movie_id on {genre_table}(movie_id);
             create index idx_year on {movie_table}(year);
             create fulltext index idx_title on {movie_table}(title);
             create fulltext index idx_name on {director_table}(director_name);
        """
        print("Creating tables...")
        cur.execute(create_sql)
        conn.commit()

        movie_insert = f"""INSERT INTO {movie_table} (title, eng_title, year, country, m_type, genre, status, director, company)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        director_insert = f"""INSERT INTO {director_table} (director_name) VALUES (%s) 
                              ON DUPLICATE KEY UPDATE director_name=director_name;"""
        genre_insert = f"""INSERT INTO {genre_table} (movie_id, genre_name) VALUES (%s, %s);"""
        moviedirector_insert = f"""INSERT INTO {moviedirector_table} (movie_id, director_id) VALUES (%s, %s);"""
        select_last_movie_id_sql = f"""select max(movie_id) as id from {movie_table};"""
        select_last_director_id_sql = f"""select max(director_id) as id from {director_table};"""        
        movies_data = []
        directors_data = []  
        genres_data = []
       

        print("Processing rows...")
        for df in dfs :
            for i, r in df.iterrows():
                row = tuple(r)
                title = row[0]
                eng_title = row[1]
                year = (row[2]) 
                country = row[3]
                m_type = row[4]
                genre = row[5]
                status = row[6]
                director = row[7]
                company = row[8]

                # Insert movie data
                movies_data.append((title, eng_title, year, country, m_type, genre, status, director, company))

                
                directors_data.append(director)
                
                genres_data.append(genre)

        try:
            print("Inserting movies...")
            cur.executemany(movie_insert, movies_data)
            conn.commit()

            cur.execute(select_last_movie_id_sql)
            last_movie_id = cur.fetchone()["id"]
            first_movie_id = last_movie_id - len(movies_data) + 1

            genre_inserts = []
            for movie_id in range(first_movie_id, last_movie_id + 1):
                row_index = movie_id - first_movie_id
                if genres_data[row_index]:
                    for genre_name in genres_data[row_index].split(','):
                        genre_inserts.append((movie_id, genre_name.strip()))

            print("Inserting genres...")
            if genre_inserts:
                cur.executemany(genre_insert, genre_inserts)
                conn.commit()
            else:
                print("No genre data to insert.")

            unique_directors = set()
            for directors in directors_data:
                if directors is None:
                    continue
                for director in directors.split(","):
                    unique_directors.add(director.strip())
            unique_directors = list(unique_directors)
           

            print("Inserting directors...")
            cur.executemany(director_insert,unique_directors)
            conn.commit()
            print("Director insert complete")
            cur.execute(select_last_director_id_sql)
            last_director_id = cur.fetchone()["id"] # director_id 중 가장 큰 값. 마지막 director_id ex) 10000
            first_director_id = last_director_id - len(unique_directors) + 1 # 10000 - 10000 + 1 하면 첫번째 director_id
            print("complete.")
            director_id_map = {}
            for director_id in range(first_director_id, last_director_id + 1): # range(1,10001) 1 ~ 10000
                row_index = director_id - first_director_id # row_index = 1 - 1 = 0, 2-1 = 1, 이런식으로 증가
                director_id_map[unique_directors[row_index]] = director_id 
            print("complete.")
            movie_director_data = []
            for movie_id in range(first_movie_id, last_movie_id + 1):
                row_index = movie_id - first_movie_id
               
                if directors_data[row_index] is None:
                    continue
                for director in directors_data[row_index].split(","):
                    director = director.strip()
                    if director in director_id_map:
                        movie_director_data.append((movie_id, director_id_map[director]))
                    else:
                        print(f"Director '{director}' not found in director_id_map.")
            print(len(movie_director_data))
            print("complete.")
            print("Inserting movie-director relations...")
            
            cur.executemany(moviedirector_insert, movie_director_data)
            conn.commit()
            

        except Exception as e:
            print(f"Error during data insertion: {e}")
            conn.rollback()

        close_db(conn, cur)
        print("Done.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    read_excel_into_mysql()
