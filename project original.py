import pandas as pd
import numpy as np
from db_conn import *

def read_excel_into_mysql():
    try:
        excel_file = "movie_list.xls"
        print("엑셀 파일을 로딩 중입니다...")

        df_tab1 = pd.read_excel(excel_file, sheet_name='movie1', skiprows=4)
        df_tab2 = pd.read_excel(excel_file, sheet_name='movie2', header=None)
        print("데이터베이스 연결을 여는 중입니다...")
        conn, cur = open_db()
        
        df_tab1 = df_tab1.replace({np.nan: None})
        df_tab2 = df_tab2.replace({np.nan: None})
        dfs = [df_tab1, df_tab2]
        print("두 시트의 데이터를 병합 중입니다...")

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
                PRIMARY KEY (movie_id, genre_name),
                FOREIGN KEY (movie_id) REFERENCES {movie_table}(movie_id) ON DELETE CASCADE
            );
            CREATE TABLE {director_table}(
                director_id INT AUTO_INCREMENT PRIMARY KEY,
                director_name VARCHAR(255)
            );
            CREATE TABLE {moviedirector_table}(
                movie_id INT,
                director_ids VARCHAR(255),
                PRIMARY KEY (movie_id),
                FOREIGN KEY (movie_id) REFERENCES {movie_table}(movie_id) ON DELETE CASCADE
            );
            CREATE INDEX idx_movie_id ON {genre_table}(movie_id);
            CREATE INDEX idx_year ON {movie_table}(year);
            CREATE INDEX idx_directorid_ ON {moviedirector_table}(director_ids);
            CREATE FULLTEXT INDEX idx_title ON {movie_table}(title);
            CREATE FULLTEXT INDEX idx_name ON {director_table}(director_name);
        """
        print("테이블을 생성 중입니다...")
        cur.execute(create_sql)
        conn.commit()

        movie_insert = f"""INSERT INTO {movie_table} (title, eng_title, year, country, m_type, status, company)
                           VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        director_insert = f"""INSERT INTO {director_table} (director_name) VALUES (%s)
                              ON DUPLICATE KEY UPDATE director_name=director_name;"""
        genre_insert = f"""INSERT INTO {genre_table} (movie_id, genre_name) VALUES (%s, %s);"""
        moviedirector_insert = f"""INSERT INTO {moviedirector_table} (movie_id, director_ids) VALUES (%s, %s)
                                   ON DUPLICATE KEY UPDATE director_ids = VALUES(director_ids);"""
        select_last_movie_id_sql = f"""SELECT MAX(movie_id) AS id FROM {movie_table};"""
        select_last_director_id_sql = f"""SELECT MAX(director_id) AS id FROM {director_table};"""
        
        movies_data = []
        directors_data = []
        genres_data = []

        print("행을 처리 중입니다...")
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

                # 영화 데이터 삽입
                movies_data.append((title, eng_title, year, country, m_type, status, company))
                directors_data.append(director)
                genres_data.append(genre)

        try:
            print("영화를 삽입 중입니다...")
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
            genres_data = [x for x in genres_data if x and x[1]]  # None 값 제거 및 유효한 값 확인
            
            print("장르를 삽입 중입니다...")
            if genres_data:
                cur.executemany(genre_insert, genres_data)
                conn.commit()
            else:
                print("삽입할 장르 데이터가 없습니다.")

            print("감독을 삽입 중입니다...")
            unique_directors = set()
            for directors in directors_data:
                if directors:
                    for director in directors.split(","):
                        unique_directors.add(director.strip())

            unique_directors = list(unique_directors)
            cur.executemany(director_insert, [(director,) for director in unique_directors])
            conn.commit()

            cur.execute(select_last_director_id_sql)
            last_director_id = cur.fetchone()["id"]
            first_director_id = last_director_id - len(unique_directors) + 1

            director_id_map = {}
            for director_id in range(first_director_id, last_director_id + 1):
                row_index = director_id - first_director_id
                director_id_map[unique_directors[row_index]] = director_id

            
            print("영화-감독 관계를 삽입 중입니다...")
            movie_director_data = {}  
            for movie_id in range(first_movie_id, last_movie_id + 1):
                row_index = movie_id - first_movie_id
                if directors_data[row_index] is None:
                    continue
                director_id = [str(director_id_map[director.strip()]) for director in directors_data[row_index].split(",")]
                if movie_id in movie_director_data:
                    movie_director_data[movie_id].extend(director_id)
                else:
                    movie_director_data[movie_id] = director_id

            # 중복 제거 및 결합
            final_movie_director_data = []
            for movie_id, director_id in movie_director_data.items():
                unique_director_id = sorted(set(director_id))
                director_id_str = ",".join(unique_director_id)
                final_movie_director_data.append((movie_id, director_id_str))

            cur.executemany(moviedirector_insert, final_movie_director_data)
            conn.commit()
            print("영화-감독 관계 삽입 완료")

        except Exception as e:
            print(f"데이터 삽입 중 오류 발생: {e}")
            conn.rollback()

        close_db(conn, cur)
        print("완료되었습니다.")
    except Exception as e:
        print(f"오류가 발생했습니다: {e}")

if __name__ == '__main__':
    read_excel_into_mysql()
