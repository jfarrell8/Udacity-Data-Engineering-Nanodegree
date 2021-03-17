import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist VARCHAR,
                                                                            auth VARCHAR,
                                                                            firstName VARCHAR,
                                                                            gender VARCHAR,
                                                                            iteminSession INT,
                                                                            lastName VARCHAR,
                                                                            length NUMERIC,
                                                                            level VARCHAR,
                                                                            location VARCHAR,
                                                                            method VARCHAR,
                                                                            page VARCHAR,
                                                                            registration BIGINT,
                                                                            sessionID INT,
                                                                            song VARCHAR,
                                                                            status INT,
                                                                            ts BIGINT,
                                                                            userAgent VARCHAR,
                                                                            userID INT);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (num_songs BIGINT,
                                                                            artist_id VARCHAR,
                                                                            artist_latitude VARCHAR,
                                                                            artist_location VARCHAR,
                                                                            artist_name VARCHAR,
                                                                            song_id VARCHAR,
                                                                            title VARCHAR,
                                                                            duration NUMERIC,
                                                                            year INT);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,\
                                                                       start_time TIMESTAMP NOT NULL, \
                                                                       user_id INT NOT NULL, \
                                                                       level VARCHAR, \
                                                                       song_id VARCHAR, \
                                                                       artist_id VARCHAR, \
                                                                       session_id VARCHAR, \
                                                                       location VARCHAR, \
                                                                       user_agent VARCHAR);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY, \
                                                          first_name VARCHAR, \
                                                          last_name VARCHAR, \
                                                          gender VARCHAR, \
                                                          level VARCHAR);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY, \
                                                          title VARCHAR, \
                                                          artist_id VARCHAR, \
                                                          year INT, \
                                                          duration NUMERIC);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY, \
                                                              name VARCHAR, \
                                                              location VARCHAR, \
                                                              latitude VARCHAR, \
                                                              longitude VARCHAR);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY, \
                                                         hour INT NOT NULL, \
                                                         day INT NOT NULL, \
                                                         week INT NOT NULL, \
                                                         month INT NOT NULL, \
                                                         year INT NOT NULL, \
                                                         weekday INT NOT NULL);
""")

# STAGING TABLES
staging_events_copy = ("""COPY staging_events FROM {}
                          CREDENTIALS 'aws_iam_role={}'
                          REGION 'us-west-2'
                          JSON {};""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs FROM {}
                          CREDENTIALS 'aws_iam_role={}'
                          REGION 'us-west-2'
                          JSON 'auto';""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, \
                                                   artist_id, session_id, location, user_agent) \
                            VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) \
                        VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (user_id) \
                        DO UPDATE SET level=EXCLUDED.level
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) \
                        VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (song_id) \
                        DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                          VALUES (%s, %s, %s, %s, %s) \
                          ON CONFLICT (song_id) \
                          DO NOTHING
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                        VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s) \
                        ON CONFLICT (start_time) \
                        DO NOTHING
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
