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

