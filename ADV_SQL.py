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
