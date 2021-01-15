import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# VARIABLES
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSON_PATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES
drop_statement = "DROP TABLE IF EXISTS {}"  # drop statement prefix
staging_events_table_drop = drop_statement.format('events_stage')
staging_songs_table_drop = drop_statement.format('songs_stage')
songplay_table_drop = drop_statement.format('songplays')
user_table_drop = drop_statement.format('users')
song_table_drop = drop_statement.format('songs')
artist_table_drop = drop_statement.format('artists')
time_table_drop = drop_statement.format('time')

# CREATE TABLES

staging_events_table_create = \
("""CREATE TABLE IF NOT EXISTS events_staging (
          artist                  VARCHAR       DISTKEY,
          auth                    VARCHAR,         
          firstName               VARCHAR,         
          gender                  VARCHAR,         
          itemInSession           INTEGER,         
          lastName                VARCHAR,         
          length                  FLOAT,         
          level                   VARCHAR,         
          location                VARCHAR,    
          method                  VARCHAR,         
          page                    VARCHAR,         
          registration            BIGINT,         
          sessionId               INTEGER,         
          song                    VARCHAR,         
          status                  INTEGER,         
          ts                      BIGINT,         
          userAgent               VARCHAR,         
          userId                  VARCHAR         
);
""")

staging_songs_table_create = \
("""CREATE TABLE IF NOT EXISTS songs_staging (
      num_songs           INTEGER,         
      artist_id           VARCHAR,         
      artist_latitude     FLOAT,         
      artist_longitude    FLOAT,         
      artist_location     VARCHAR,    
      artist_name         VARCHAR       DISTKEY,
      song_id             VARCHAR,         
      title               VARCHAR,    
      duration            FLOAT,           
      year                INTEGER
);
""")

songplay_table_create = \
("""CREATE TABLE IF NOT EXISTS songplays (
      songplays_id        INTEGER IDENTITY(0,1)   NOT NULL,
      start_time          TIMESTAMP               SORTKEY,
      user_id             INTEGER                 NOT NULL,
      level               VARCHAR,
      song_id             VARCHAR                 NOT NULL    DISTKEY,
      artist_id           VARCHAR                 NOT NULL,
      session_id          INTEGER                 NOT NULL,
      location            VARCHAR,
      user_agent          VARCHAR                 
);
""")

user_table_create = \
("""CREATE TABLE IF NOT EXISTS users (
      user_id             INTEGER     NOT NULL,
      first_name          VARCHAR     SORTKEY,
      last_name           VARCHAR,     
      gender              VARCHAR,     
      level               VARCHAR     
)
DISTSTYLE ALL;
""")

song_table_create = \
("""CREATE TABLE IF NOT EXISTS songs (
      song_id             VARCHAR     NOT NULL    DISTKEY,
      artist_id           VARCHAR,
      title               VARCHAR     SORTKEY,
      year                INTEGER,     
      duration            NUMERIC     
);
""")

artist_table_create = \
("""CREATE TABLE IF NOT EXISTS artists (
      artist_id           VARCHAR     NOT NULL,
      name                VARCHAR     SORTKEY,
      location            VARCHAR,     
      latitude            NUMERIC,     
      longitude           NUMERIC     
)
DISTSTYLE ALL;
""")

time_table_create = \
("""CREATE TABLE IF NOT EXISTS time (
       start_time           TIMESTAMP       SORTKEY,
       hour                 SMALLINT,        
       day                  SMALLINT,        
       month                SMALLINT,        
       year                 SMALLINT,        
       weekday              SMALLINT        
)
DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""COPY events_staging FROM {}
                          IAM_ROLE {}
                          JSON {};
                       """).format(LOG_DATA, ARN, LOG_JSON_PATH)

staging_songs_copy = ("""COPY songs_staging FROM {}
                         IAM_ROLE {}
                         JSON 'auto';
                      """).format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = \
("""INSERT INTO songplays (
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent
                        )
    SELECT DISTINCT TIMESTAMP 'epoch' + es.ts/1000 * \
                     INTERVAL '1 second' AS start_time,
                     CAST(es.userId AS INTEGER) AS user_id,
                     es.level AS level,
                     ss.song_id AS song_id,
                     ss.artist_id AS artist_id,
                     es.sessionId AS session_id,
                     es.location AS location,
                     es.userAgent AS user_agent
    FROM events_staging AS es
    JOIN songs_staging AS ss ON (es.artist = ss.artist_name)
    WHERE es.page = 'NextSong';      
""")

user_table_insert = \
("""INSERT INTO users (user_id,
                       first_name,
                       last_name,
                       gender,
                       level
                    )
    SELECT DISTINCT CAST(userId AS INTEGER) AS user_id,
                    firstName AS first_name,
                    lastName AS last_name,
                    gender,
                    level
    FROM events_staging
    WHERE page = 'NextSong';
""")

song_table_insert = \
("""INSERT INTO songs (song_id,
                       artist_id,
                       title,
                       year,
                       duration
                    )
    SELECT DISTINCT song_id,
                    artist_id,
                    title,
                    year,
                    duration
    FROM songs_staging;
                                        
""")

artist_table_insert = \
("""INSERT INTO artists (artist_id,
                         name,
                         location,
                         latitude,
                         longitude
                      )
    SELECT DISTINCT artist_id,
                    artist_name AS name,
                    artist_location AS location,
                    artist_latitude AS latitude,
                    artist_longitude AS longitude
   FROM songs_staging;
""")

time_table_insert = \
("""INSERT INTO time (start_time,
                      hour,
                      day,
                      month,
                      year,
                      weekday
                    )
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * \
           INTERVAL '1 second' AS start_time,
           EXTRACT(hour FROM start_time) AS hour,
           EXTRACT(day FROM start_time) AS day,
           EXTRACT(month FROM start_time) AS month,
           EXTRACT(year FROM start_time) AS year,
           EXTRACT(weekday FROM start_time) AS weekday
    FROM events_staging
    WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    ]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
    ]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]