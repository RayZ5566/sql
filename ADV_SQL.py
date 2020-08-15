from google.cloud import bigquery
import os

#setting environment
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/user/Desktop/Kaggle/SQL/t-variety-286403-e5a0529c090d.json'
# Create a "Client" object
client = bigquery.Client()
# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset('hacker_news', project='bigquery-public-data')
# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)
# Construct a reference to the "comments" table
table_ref = dataset_ref.table('comments')
# API request - fetch the table
table = client.get_table(table_ref)
# Preview the first five lines of the table
client.list_rows(table, max_results=5).to_dataframe()

# Construct a reference to the "stories" table
table_ref = dataset_ref.table('stories')
# API request - fetch the table
table = client.get_table(table_ref)
# Preview the first five lines of the table
client.list_rows(table, max_results=5).to_dataframe()



#The query below pulls information from the stories and comments tables to create a table showing all stories posted on January 1, 2012, 
#along with the corresponding number of comments. 
#We use a LEFT JOIN so that the results include stories that didn't receive any comments.

# Query to select all stories posted on January 1, 2012, with number of comments
join_query = """
             WITH c AS
             (
                 SELECT parent, COUNT(*) as num_comments
                 FROM `bigquery-public-data.hacker_news.comments`
                 GROUP BY parent
                 )
             SELECT s.id as story_id, s.by, s.title, c.num_comments
             FROM `bigquery-public-data.hacker_news.stories` AS s
             LEFT JOIN c
             ON s.id = c.parent
             WHERE EXTRACT(DATE FROM s.time_ts) = '2012-01-01'
             ORDER BY c.num_comments DESC
             """

# Run the query, and return a pandas DataFrame
join_result = client.query(join_query).result().to_dataframe()
join_result.head()
join_result.tail()


#we write a query to select all usernames corresponding to users who wrote stories or comments on January 1, 2014. 
#We use UNION DISTINCT (instead of UNION ALL) to ensure that each user appears in the table at most once.

# Query to select all users who posted stories or comments on January 1, 2014

union_query = """
              SELECT c.by
              FROM `bigquery-public-data.hacker_news.comments` AS c
              WHERE EXTRACT (DATE FROM c.time_ts) = '2014-01-01'
              UNION DISTINCT
              SELECT s.by
              FROM `bigquery-public-data.hacker_news.stories` AS s
              WHERE EXTRACT (DATE FROM s.time_ts) = '2014-01-01'
              """
# Run the query, and return a pandas DataFrame
union_result = client.query(union_query).result().to_dataframe()
union_result.head()            

# Number of users who posted stories or comments on January 1, 2014
len(union_result)

#
# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "stackoverflow" dataset
dataset_ref = client.dataset("stackoverflow", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "posts_questions" table
table_ref = dataset_ref.table("posts_questions")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the table
client.list_rows(table, max_results=5).to_dataframe()

# Construct a reference to the "posts_answers" table
table_ref = dataset_ref.table("posts_answers")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the table
client.list_rows(table, max_results=5).to_dataframe()


#You're interested in exploring the data to have a better understanding of how long it generally takes for questions to receive answers. 
#Armed with this knowledge, you plan to use this information to better design the order in which questions are presented to Stack Overflow users.

#With this goal in mind, you write the query below, which focuses on questions asked in January 2018. It returns a table with two columns:




# Your code here
correct_query = """
              SELECT q.id AS q_id,
                  MIN(TIMESTAMP_DIFF(a.creation_date, q.creation_date, SECOND)) as time_to_answer
              FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                  LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
              ON q.id = a.parent_id
              WHERE q.creation_date >= '2018-01-01' and q.creation_date < '2018-02-01'
              GROUP BY q_id
              ORDER BY time_to_answer
              """

# Run the query, and return a pandas DataFrame
correct_result = client.query(correct_query).result().to_dataframe()
print("Percentage of answered questions: %s%%" % \
      (sum(correct_result["time_to_answer"].notnull()) / len(correct_result) * 100))
print("Number of questions:", len(correct_result))




# Is it more common for users to first ask questions or provide answers? 
#After signing up, how long does it take for users to first interact with the website? 
#To explore this further, you draft the (partial) query in the code cell below.


# Your code here
q_and_a_query = """
                SELECT q.owner_user_id AS owner_user_id,
                    MIN(q.creation_date) AS q_creation_date,
                    MIN(a.creation_date) AS a_creation_date
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                FULL JOIN  `bigquery-public-data.stackoverflow.posts_answers` AS a
                ON q.owner_user_id = a.owner_user_id 
                WHERE q.creation_date >= '2019-01-01' AND q.creation_date < '2019-02-01' 
                    AND a.creation_date >= '2019-01-01' AND a.creation_date < '2019-02-01'
                GROUP BY owner_user_id
                """
                
#Now you'll address a more realistic (and complex!) scenario. 
#To answer this question, you'll need to pull information from three different tables! This syntax very similar to the case when we have to join only two tables. For instance, consider the three tables below.            
#Write a query that returns the following columns:

#id - the IDs of all users who created Stack Overflow accounts in January 2019 (January 1, 2019, to January 31, 2019, inclusive)
#q_creation_date - the first time the user posted a question on the site; if the user has never posted a question, the value should be null
#a_creation_date - the first time the user posted a question on the site; if the user has never posted a question, the value should be null


three_tables_query = """
                SELECT u.id AS id,
                    MIN(q.creation_date) AS q_creation_date,
                    MIN(a.creation_date) AS a_creation_date
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                    FULL JOIN  `bigquery-public-data.stackoverflow.posts_answers` AS a
                        ON q.owner_user_id = a.owner_user_id 
                    RIGHT JOIN `bigquery-public-data.stackoverflow.users` AS u
                        ON q.owner_user_id = u.id
                WHERE u.creation_date >= '2019-01-01' and u.creation_date <'2019-02-01'
                GROUP BY id
                """

#In the code cell below, write a query that returns a table with a single column:
#- `owner_user_id` - the IDs of all users who posted at least one question or answer on January 1, 2019.  Each user ID should appear at most once.

#In the `posts_questions` (and `posts_answers`) tables, you can get the ID of the original poster from the `owner_user_id` column.  Likewise, the date of the original posting can be found in the `creation_date` column.  
              
all_users_query = """
                  SELECT q.owner_user_id
                  FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                  WHERE EXTRACT(DATE FROM q.creation_date) = '2019-01-01'
                  UNION DISTINCT   
                  SELECT a.owner_user_id
                  FROM `bigquery-public-data.stackoverflow.posts_answers` AS a
                  WHERE EXTRACT(DATE FROM a.creation_date) = '2019-01-01'
                  """
#----------------------------------------------------------------------------#              
####analytic-functions

# Create a "Client" object
client = bigquery.Client()
# Construct a reference to the "san_francisco" dataset
dataset_ref = client.dataset("san_francisco", project="bigquery-public-data")
# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)
# Construct a reference to the "bikeshare_trips" table
table_ref = dataset_ref.table("bikeshare_trips")
# API request - fetch the table
table = client.get_table(table_ref)
# Preview the first five lines of the table
client.list_rows(table, max_results=5).to_dataframe()


#We'll work with the San Francisco Open Data dataset. We begin by reviewing the first several rows of the bikeshare_trips table. 
#(The corresponding code is hidden, but you can un-hide it by clicking on the "Code" button below.)

# Query to count the (cumulative) number of trips per day
num_trips_query = """
                  WITH trips_by_day AS
                  (
                  SELECT DATE(start_date) AS trip_date,
                      COUNT(*) as num_trips
                  FROM `bigquery-public-data.san_francisco.bikeshare_trips`
                  WHERE EXTRACT(YEAR FROM start_date) = 2015
                  GROUP BY trip_date
                  )
                  SELECT *,
                      SUM(num_trips)
                          OVER(
                              ORDER BY trip_date
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                              ) AS cumulative_trips
                      FROM trips_by_day
                  """

num_trips_result = client.query(num_trips_query).result().to_dataframe()
num_trips_result.head()


#The query uses a common table expression (CTE) to first calculate the daily number of trips. Then, we use SUM() as an aggregate function.

#Since there is no PARTITION BY clause, the entire table is treated as a single partition.
#The ORDER BY clause orders the rows by date, where earlier dates appear first.
#By setting the window frame clause to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, we ensure that all rows up to and including the current date are used to calculate the (cumulative) sum. 
#(Note: If you read the documentation, you'll see that this is the default behavior, and so the query would return the same result if we left out this window frame clause.)
#The next query tracks the stations where each bike began (in start_station_id) and ended (in end_station_id) the day on October 25, 2015.

start_end_query = """
                  SELECT bike_number,
                      TIME(start_date) AS trip_time,
                      FIRST_VALUE(start_station_id)
                          OVER(
                              PARTITION BY bike_number
                              ORDER BY start_date
                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                              ) AS first_station_id,
                      LAST_VALUE(end_station_id)
                          OVER(
                              PARTITION BY bike_number
                              ORDER BY start_date
                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                              ) AS last_statinon_id,
                      start_station_id,
                      end_station_id
                  FROM `bigquery-public-data.san_francisco.bikeshare_trips`
                  WHERE DATE(start_date) = '2015-10-25'
                  """
# Run the query, and return a pandas DataFrame
start_end_result = client.query(start_end_query).result().to_dataframe()
start_end_result.tail()




#EXCERCISE

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "chicago_taxi_trips" dataset
dataset_ref = client.dataset("chicago_taxi_trips", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "taxi_trips" table
table_ref = dataset_ref.table("taxi_trips")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the table
client.list_rows(table, max_results=5).to_dataframe()

#1) How can you predict the demand for taxis?
#Say you work for a taxi company, and you're interested in predicting the demand for taxis. Towards this goal, you'd like to create a plot that shows a rolling average of the daily number of taxi trips. Amend the (partial) query below to return a DataFrame with two columns:

#trip_date - contains one entry for each date from January 1, 2016, to December 31, 2017.
#avg_num_trips - shows the average number of daily trips, calculated over a window including the value for the current date, along with the values for the preceding 15 days and the following 15 days, as long as the days fit within the two-year time frame. 
#For instance, when calculating the value in this column for January 5, 2016, the window will include the number of trips for the preceding 4 days, the current date, and the following 15 days.
#This query is partially completed for you, and you need only write the part that calculates the avg_num_trips column. Note that this query uses a common table expression (CTE); if you need to review how to use CTEs, you're encouraged to check out this tutorial in the Intro to SQL micro-course.
avg_num_trips_query = """
                      WITH trips_by_day AS
                      (
                      SELECT DATE(trip_start_timestamp) AS trip_date,
                          COUNT(*) as num_trips
                      FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                      WHERE trip_start_timestamp >= '2016-01-01' AND trip_start_timestamp < '2018-01-01'
                      GROUP BY trip_date
                      ORDER BY trip_date
                      )
                      SELECT trip_date,
                          AVG(num_trips)
                          OVER (
                               ORDER BY trip_date
                               ROWS BETWEEN 15 PRECEDING and 15 FOLLOWING
                               ) AS avg_num_trips
                      FROM trips_by_day
                      """
avg_num_trips_result = client.query(avg_num_trips_query).result().to_dataframe()
avg_num_trips_result.head()  



#2) Can you separate and order trips by community area?
#The query below returns a DataFrame with three columns from the table: pickup_community_area, trip_start_timestamp, and trip_end_timestamp.

#Amend the query to return an additional column called trip_number which shows the order in which the trips were taken from their respective community areas. 
#So, the first trip of the day originating from community area 1 should receive a value of 1; the second trip of the day from the same area should receive a value of 2. Likewise, the first trip of the day from community area 2 should receive a value of 1, and so on.

trip_number_query = """
                    SELECT pickup_community_area,
                        trip_start_timestamp,
                        trip_end_timestamp,
                        RANK()
                            OVER(
                            PARTITION BY pickup_community_area
                            ORDER by trip_start_timestamp
                                ) as trip_number
                    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                    WHERE DATE(trip_start_timestamp) = '2017-05-01'
                    """
trip_number_result = client.query(trip_number_query).result().to_dataframe()
trip_number_result.head()  


#3) How much time elapses between trips?
#The (partial) query in the code cell below shows, for each trip in the selected time frame, the corresponding taxi_id, trip_start_timestamp, and trip_end_timestamp.

#Your task in this exercise is to edit the query to include an additional prev_break column that shows the length of the break (in minutes) that the driver had before each trip started (this corresponds to the time between trip_start_timestamp of the current trip and trip_end_timestamp of the previous trip). 
#Partition the calculation by taxi_id, and order the results within each partition by trip_start_timestamp.

#Some sample results are shown below, where all rows correspond to the same driver (or taxi_id). Take the time now to make sure that the values in the prev_break column make sense to you!
 
#Hint: The TIMESTAMP_DIFF() function takes three arguments, where the first (trip_start_timestamp) and the last (MINUTE) are provided for you. This function provides the time difference (in minutes) of the timestamps in the first two arguments. 
#You need only fill in the second argument, which should use the LAG() function to pull the timestamp corresponding to the end of the previous trip (for the same taxi_id).
break_time_query = """
                   SELECT taxi_id,
                       trip_start_timestamp,
                       trip_end_timestamp,
                       TIMESTAMP_DIFF(
                           trip_start_timestamp, 
                           LAG(trip_end_timestamp, 1) 
                               OVER (
                                    PARTITION BY taxi_id 
                                    ORDER BY trip_start_timestamp), 
                           MINUTE) as prev_break
                   FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                   WHERE DATE(trip_start_timestamp) = '2017-05-01' 
                   """
                   
break_time_result = client.query(break_time_query).result().to_dataframe()
break_time_result.head()


#-------------------------------------------------------------#
#NESTED AND REPEATED DATA

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "google_analytics_sample" dataset
dataset_ref = client.dataset("google_analytics_sample", project="bigquery-public-data")

# Construct a reference to the "ga_sessions_20170801" table
table_ref = dataset_ref.table("ga_sessions_20170801")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the table
client.list_rows(table, max_results=5).to_dataframe()

table.schema

# Query to count the number of transactions per browser
query = """
        SELECT device.browser AS device_browser,
            SUM(totals.transactions) as total_transactions
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
        GROUP BY device_browser
        ORDER BY total_transactions DESC
        """

# Run the query, and return a pandas DataFrame
result = client.query(query).result().to_dataframe()
result.head()



# Query to determine most popular landing point on the website
query = """
        SELECT hits.page.pagePath as path,
            COUNT(hits.page.pagePath) as counts
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`, 
            UNNEST(hits) as hits
        WHERE hits.type="PAGE" and hits.hitNumber=1
        GROUP BY path
        ORDER BY counts DESC
        """

# Run the query, and return a pandas DataFrame
result = client.query(query).result().to_dataframe()
result.head()

#EXERCISES
#1) Who had the most commits in 2016?
#GitHub is the most popular place to collaborate on software projects. A GitHub repository (or repo) is a collection of files associated with a specific project, and a GitHub commit is a change that a user has made to a repository.
#We refer to the user as a committer.

#The sample_commits table contains a small sample of GitHub commits, where each row corresponds to different commit. 
#The code cell below fetches the table and shows the first five rows of this table.

from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "github_repos" dataset
dataset_ref = client.dataset("github_repos", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "sample_commits" table
table_ref = dataset_ref.table("sample_commits")

# API request - fetch the table
sample_commits_table = client.get_table(table_ref)

# Preview the first five lines of the table
client.list_rows(sample_commits_table, max_results=5).to_dataframe()

# Print information on all the columns in the table
sample_commits_table.schema

#Write a query to find the individuals with the most commits in this table in 2016. Your query should return a table with two columns:

#committer_name - contains the name of each individual with a commit (from 2016) in the table
#num_commits - shows the number of commits the individual has in the table (from 2016)
#Sort the table, so that people with more commits appear first.

# Write a query to find the answer
max_commits_query = """
                    SELECT committer.name AS committer_name, COUNT(*) AS num_commits
                    FROM `bigquery-public-data.github_repos.sample_commits`
                    WHERE EXTRACT(year FROM committer.date) = 2016
                    GROUP BY committer_name
                    ORDER BY num_commits DESC
                    """
                    
# Run the query, and return a pandas DataFrame
result = client.query(max_commits_query).result().to_dataframe()
result.head()                    

#3) What's the most popular programming language?
#Write a query to leverage the information in the languages table to determine which programming languages appear in the most repositories. The table returned by your query should have two columns:

#language_name - the name of the programming language
#num_repos - the number of repositories in the languages table that use the programming language

# Write a query to find the answer
pop_lang_query = """
                SELECT l.name as language_name, COUNT(*) as num_repos
                FROM `bigquery-public-data.github_repos.languages`,
                    UNNEST(language) AS l
                GROUP BY language_name
                ORDER BY num_repos DESC
                 """
# Run the query, and return a pandas DataFrame
result = client.query(pop_lang_query).result().to_dataframe()
result.head()                    

#4) Which languages are used in the repository with the most languages?
#For this question, you'll restrict your attention to the repository with name 'polyrabbit/polyglot'.

#Write a query that returns a table with one row for each language in this repository. The table should have two columns:

#name - the name of the programming language
#bytes - the total number of bytes of that programming language
#Sort the table by the bytes column so that programming languages that take up more space in the repo appear first.

# Your code here
all_langs_query = """
                    SELECT l.name as name, l.bytes as bytes
                    FROM `bigquery-public-data.github_repos.languages`,
                        UNNEST(language) AS l
                    WHERE repo_name = 'polyrabbit/polyglot'
                    
                    ORDER BY bytes DESC
                  """
# Run the query, and return a pandas DataFrame
result = client.query(all_langs_query).result().to_dataframe()
result.head()                   

#---------------------------------------------------------------------#
#Writing Effcient Queries
from google.cloud import bigquery
from time import time

client = bigquery.Client()

def show_amount_of_data_scanned(query):
    # dry_run lets us see how much data the query uses without running it
    dry_run_config = bigquery.QueryJobConfig(dry_run=True)
    query_job = client.query(query, job_config=dry_run_config)
    print('Data processed: {} GB'.format(round(query_job.total_bytes_processed / 10**9, 3)))
    
def show_time_to_run(query):
    time_config = bigquery.QueryJobConfig(use_query_cache=False)
    start = time()
    query_result = client.query(query, job_config=time_config).result()
    end = time()
    print('Time to run: {} seconds'.format(round(end-start, 3)))
    
#1) Only select the columns you want.
#It is tempting to start queries with SELECT * FROM .... It's convenient because you don't need to think about which columns you need. But it can be very inefficient.

#This is especially important if there are text fields that you don't need, because text fields tend to be larger than other fields.    
star_query = "SELECT * FROM `bigquery-public-data.github_repos.contents`"
show_amount_of_data_scanned(star_query)

basic_query = "SELECT size, binary FROM `bigquery-public-data.github_repos.contents`"
show_amount_of_data_scanned(basic_query)

#2) Read less data.
#Both queries below calculate the average duration (in seconds) of one-way bike trips in the city of San Francisco.
more_data_query = """
                  SELECT MIN(start_station_name) AS start_station_name,
                      MIN(end_station_name) AS end_station_name,
                      AVG(duration_sec) AS avg_duration_sec
                  FROM `bigquery-public-data.san_francisco.bikeshare_trips`
                  WHERE start_station_id != end_station_id 
                  GROUP BY start_station_id, end_station_id
                  LIMIT 10
                  """
show_amount_of_data_scanned(more_data_query)

less_data_query = """
                  SELECT start_station_name,
                      end_station_name,
                      AVG(duration_sec) AS avg_duration_sec                  
                  FROM `bigquery-public-data.san_francisco.bikeshare_trips`
                  WHERE start_station_name != end_station_name
                  GROUP BY start_station_name, end_station_name
                  LIMIT 10
                  """
show_amount_of_data_scanned(less_data_query)


#3) Avoid N:N JOINs.
#An N:N JOIN is one where a group of rows in one table can match a group of rows in the other table. 
#Note that in general, all other things equal, this type of JOIN produces a table with many more rows than either of the two (original) tables that are being JOINed.
big_join_query = """
                 SELECT repo,
                     COUNT(DISTINCT c.committer.name) as num_committers,
                     COUNT(DISTINCT f.id) AS num_files
                 FROM `bigquery-public-data.github_repos.commits` AS c,
                     UNNEST(c.repo_name) AS repo
                 INNER JOIN `bigquery-public-data.github_repos.files` AS f
                     ON f.repo_name = repo
                 WHERE f.repo_name IN ( 'tensorflow/tensorflow', 'facebook/react', 'twbs/bootstrap', 'apple/swift', 'Microsoft/vscode', 'torvalds/linux')
                 GROUP BY repo
                 ORDER BY repo
                 """
show_time_to_run(big_join_query)

small_join_query = """
                   WITH commits AS
                   (
                   SELECT COUNT(DISTINCT committer.name) AS num_committers, repo
                   FROM `bigquery-public-data.github_repos.commits`,
                       UNNEST(repo_name) as repo
                   WHERE repo IN ( 'tensorflow/tensorflow', 'facebook/react', 'twbs/bootstrap', 'apple/swift', 'Microsoft/vscode', 'torvalds/linux')
                   GROUP BY repo
                   ),
                   files AS 
                   (
                   SELECT COUNT(DISTINCT id) AS num_files, repo_name as repo
                   FROM `bigquery-public-data.github_repos.files`
                   WHERE repo_name IN ( 'tensorflow/tensorflow', 'facebook/react', 'twbs/bootstrap', 'apple/swift', 'Microsoft/vscode', 'torvalds/linux')
                   GROUP BY repo
                   )
                   SELECT commits.repo, commits.num_committers, files.num_files
                   FROM commits 
                   INNER JOIN files
                       ON commits.repo = files.repo
                   ORDER BY repo
                   """
show_time_to_run(small_join_query)                   