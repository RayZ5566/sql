from google.cloud import bigquery
import os

#setting environment
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/user/Desktop/Kaggle/SQL/t-variety-286403-e5a0529c090d.json'


client = bigquery.Client()
#loeading dataset
dataset_ref = client.dataset('hacker_news', project='bigquery-public-data')
dataset = client.get_dataset(dataset_ref)
tables = list(client.list_tables(dataset))

for table in tables:
    print(table.table_id)
    
table_ref = dataset_ref.table('full')
table = client.get_table(table_ref)

table.schema

client.list_rows(table, max_results=5).to_dataframe()

client.list_rows(table, selected_fields=table.schema[:4], max_results=5).to_dataframe()



query3 = """
        SELECT score, title
        FROM `bigquery-public-data.hacker_news.full`
        WHERE type = "job" 
        """
dry_run_config = bigquery.QueryJobConfig(dry_run = True)
dry_run_query_job = client.query(query3, job_config=dry_run_config)
print('this query will process {} bytes'.format(dry_run_query_job.total_bytes_processed))

# Only run the query if it's less than 1 MB
one_mb = 1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=one_mb)
# Set up the query (will only run if it's less than 1 MB)
safe_query_job = client.query(query3, job_config=safe_config)
# API request - try to run the query, and return a pandas DataFrame
#GET an ERROR!!!!
safe_query_job.to_dataframe()

# Only run the query if it's less than 1 GB
one_gb = 1000*1000*1000
safe_config1 = bigquery.QueryJobConfig(maximum_bytes_billed = one_gb)
# Set up the query (will only run if it's less than 1 GB)
safe_query_job1 = client.query(query3, job_config=safe_config1)
# API request - try to run the query, and return a pandas DataFrame
job_post_scores = safe_query_job1.to_dataframe()
# Print average score for job posts
job_post_scores.score.mean()


#-----------------------------------------------------#
#Chicago
client = bigquery.Client()
dataset_ref = client.dataset('chicago_crime', project='bigquery-public-data')
dataset = client.get_dataset(dataset_ref)

##How many tables are in the Chicago Crime dataset?
tables = list(client.list_tables(dataset))
for table in tables:
    print(table.table_id)
    
##How many columns in the `crime` table have `TIMESTAMP` data?

table_ref = dataset_ref.table('crime')
table = client.get_table(table_ref)

table.schema


##If you wanted to create a map with a dot at the location of each crime, 
##what are the names of the two fields you likely need to pull out of the crime 
##table to plot the crimes on a map?
client.list_rows(table, max_results=5).to_dataframe()
client.list_rows(table, selected_fields=table.schema[-3:], max_results = 5).to_dataframe()


#-----------------------------------------------#
#global_air_quality
#What are all the U.S. cities in the OpenAQ dataset?
client = bigquery.Client()
dataset_ref =client.dataset('openaq', project='bigquery-public-data')
dataset = client.get_dataset(dataset_ref)
tables = list(client.list_tables(dataset))
for table in tables:
    print(table.table_id)
table_ref = dataset_ref.table('global_air_quality')
table = client.get_table(table_ref)
table.schema
client.list_rows(table, max_results=5).to_dataframe()
# Query to select all the items from the "city" column where the "country" column is 'US'
query = """
        SELECT city
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """
client = bigquery.Client()
query_job = client.query(query)
us_cities = query_job.to_dataframe()
us_cities.city.value_counts().head()

query1 = """
        SELECT city, country
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """
client = bigquery.Client()
query_job = client.query(query1)
us_cities = query_job.to_dataframe()
us_cities.city.value_counts().head()


query2 = """
        SELECT *
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """
query_job1 = client.query(query2)
cities = query_job1.to_dataframe()
cities.city.value_counts().head()
cities.country.value_counts().head()
cities.location.value_counts().head()

# Query to select countries with units of "ppm"
query3 = """
        select country
        from `bigquery-public-data.openaq.global_air_quality`
        where unit = 'ppm'
        """
query_job3 = client.query(query3)
ppm = query_job3.to_dataframe()
ppm.country.value_counts().head()

# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config2 = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
first_query_job = client.query(query3, job_config=safe_config2)
# API request - run the query, and return a pandas DataFrame
first_results = first_query_job.to_dataframe()
# View top few rows of results
print(first_results.head())

# Query to select all columns where pollution levels are exactly 0
query4 = """
        SELECT *
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE value = 0
        """

# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query4, job_config=safe_config)

# API request - run the query and return a pandas DataFrame
zero_pollution_results = query_job.to_dataframe()

print(zero_pollution_results.head())
