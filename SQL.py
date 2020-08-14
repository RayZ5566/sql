from google.cloud import bigquery
import os

#setting environment
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/user/Desktop/Kaggle/SQL/t-variety-286403-e5a0529c090d.json'

#HACKER_NEWS
client = bigquery.Client()
#loeading dataset
dataset_ref = client.dataset('hacker_news', project='bigquery-public-data')
dataset = client.get_dataset(dataset_ref)
tables = list(client.list_tables(dataset))

for table in tables:
    print(table.table_id)
# Construct a reference to the "full" table    
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


## Construct a reference to the "comments" table
table_ref = dataset_ref.table('comments')
# API request - fetch the table
table = client.get_table(table_ref)
#preview the fitst five lines of the 'comments' table
client.list_rows(table, max_results=5).to_dataframe()

# Query to select comments that received more than 10 replies
query_popular = """
                select parent, count(id)
                from `bigquery-public-data.hacker_news.comments`
                group by parent
                having count(id) > 10
                """

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed = 10**10)
query_job = client.query(query_popular, job_config=safe_config)

popular_comments = query_job.to_dataframe()

popular_comments.head()

# Improved version of earlier query, now with aliasing & improved readability
query_improved = """
                 SELECT parent, COUNT(1) AS NumPosts
                 FROM `bigquery-public-data.hacker_news.comments`
                 GROUP BY parent
                 HAVING COUNT(1) > 10
                 """
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query_improved, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
improved_df = query_job.to_dataframe()

# Print the first five rows of the DataFrame
improved_df.head()



query_good = """
             SELECT parent, COUNT(id)
             FROM `bigquery-public-data.hacker_news.comments`
             GROUP BY parent
             """
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query_good, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
improved_df = query_job.to_dataframe()

# Print the first five rows of the DataFrame
improved_df.head()

#ERROR!!!
query_bad = """
            SELECT author, parent, COUNT(id)
            FROM `bigquery-public-data.hacker_news.comments`
            GROUP BY parent
            """
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query_bad, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
improved_df = query_job.to_dataframe()

# Print the first five rows of the DataFrame
improved_df.head()

# Query to select prolific commenters and post counts
prolific_commenters_query = """
                            select author, count(id) as numposts
                            from `bigquery-public-data.hacker_news.comments`
                            group by author
                            having count(id) >10000
                            """
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed = 10**10)
query_job = client.query(prolific_commenters_query, job_config=safe_config)
prolific_commenters = query_job.to_dataframe()
prolific_commenters.head()


#How many comments have been deleted? (If a comment was deleted, the deleted column in the comments table will have the value True.)
prolific_commenters_query = """
                            select deleted, count(id) as numposts
                            from `bigquery-public-data.hacker_news.comments`
                            group by deleted
                            """
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed = 10**10)
query_job = client.query(prolific_commenters_query, job_config=safe_config)
deleted_comments = query_job.to_dataframe()
deleted_comments.head()
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


#---------------------------------------------------------#
#Example: Which day of the week has the most fatal motor accidents?
# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "nhtsa_traffic_fatalities" dataset
dataset_ref = client.dataset("nhtsa_traffic_fatalities", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "accident_2015" table
table_ref = dataset_ref.table("accident_2015")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "accident_2015" table
client.list_rows(table, max_results=5).to_dataframe()

# Query to find out the number of accidents for each day of the week
query = """
        select count(consecutive_number) as num_accidents,
               extract(dayofweek from timestamp_of_crash) as day_of_week
       from `bigquery-public-data.nhtsa_traffic_fatalities.accident_2015`
       group by day_of_week
       order by num_accidents desc
       """
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**9)
query_job = client.query(query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
accidents_by_day = query_job.to_dataframe()

# Print the DataFrame
accidents_by_day


# Government expenditure on education
# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "world_bank_intl_education" dataset
dataset_ref = client.dataset("world_bank_intl_education", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "international_education" table
table_ref = dataset_ref.table("international_education")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "international_education" table
client.list_rows(table, max_results=5).to_dataframe()

# Query to find out which countries spend the largest fraction of GDP on education
country_spend_pct_query = """
                          select country_name, 
                                 avg(value) as avg_ed_spending_pct
                          from `bigquery-public-data.world_bank_intl_education.international_education`
                          where indicator_code = 'SE.XPD.TOTL.GD.ZS' and year >= 2010 and year <= 2017
                          group by country_name
                          order by avg_ed_spending_pct desc
                          """
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
country_spend_pct_query_job = client.query(country_spend_pct_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
country_spending_results = country_spend_pct_query_job.to_dataframe()

# View top few rows of results
print(country_spending_results.head()) 
                          

#Identify interesting codes to explore
code_count_query = """
                   select count(country_name) as num_rows,
                          indicator_name,
                          indicator_code
                   from `bigquery-public-data.world_bank_intl_education.international_education`
                   where year = 2016
                   group by indicator_name, indicator_code
                   having num_rows >= 175
                   order by num_rows desc
                   """
# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
code_count_query_job = client.query(code_count_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
code_count_results = code_count_query_job.to_dataframe()

# View top few rows of results
print(code_count_results.head())


#-----------As With-----------------------------------------------------------#
#How many Bitcoin transactions are made per month?
# Construct a reference to the "crypto_bitcoin" dataset
dataset_ref = client.dataset("crypto_bitcoin", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "transactions" table
table_ref = dataset_ref.table("transactions")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "transactions" table
client.list_rows(table, max_results=5).to_dataframe()

# Query to select the number of transactions per date, sorted by date
query_with_cte = """
                 WITH time As
                 (
                     SELECT date(block_timestamp) as trans_date
                     FROM `bigquery-public-data.crypto_bitcoin.transactions`
                     )
                 SELECT count(1) AS transactions,
                        trans_date
                 FROM time
                 GROUP BY trans_date
                 ORDER BY trans_date
                 """
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query_with_cte, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
transactions_by_date = query_job.to_dataframe()

# Print the first five rows
transactions_by_date.head()

# raw results to show us the number of Bitcoin transactions per day over the whole timespan of this dataset.
transactions_by_date.set_index('trans_date').plot()


#taxi trips in the city of Chicago.
# Construct a reference to the "chicago_taxi_trips" dataset
dataset_ref = client.dataset("chicago_taxi_trips", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

tables = list(client.list_tables(dataset))
for table in tables:
    print(table.table_id)
# Construct a reference to the "taxi_trips" table
table_ref = dataset_ref.table("taxi_trips")
# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "transactions" table
client.list_rows(table, max_results=5).to_dataframe()


rides_per_year_query = """
                       SELECT EXTRACT(year FROM trip_start_timestamp) as year,
                              COUNT(unique_key) as num_trips
                      FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                      GROUP BY year
                      ORDER BY count(unique_key) DESC
                      """
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
rides_per_year_query_job = client.query(rides_per_year_query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
rides_per_year_result = rides_per_year_query_job.to_dataframe()

# Print the first five rows
rides_per_year_result.head()
# View results
print(rides_per_year_result)

#You'd like to take a closer look at rides from 2017.
rides_per_month_query = """
                       SELECT EXTRACT(month FROM trip_start_timestamp) as month,
                              COUNT(unique_key) as num_trips
                      FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                      WHERE EXTRACT(year FROM trip_start_timestamp) = 2017
                      GROUP BY month
                      ORDER BY count(unique_key) DESC
                      """
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
rides_per_month_query_job = client.query(rides_per_month_query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
rides_per_month_result = rides_per_month_query_job.to_dataframe()

# Print the first five rows
rides_per_month_result.head()
# View results
print(rides_per_month_result)

#Write a query that shows, for each hour of the day in the dataset, the corresponding number of trips and average speed.
speeds_query = """
               WITH RelevantRides AS
                (
                    SELECT EXTRACT(hour FROM trip_start_timestamp) as hour_of_day,
                           trip_miles,
                           trip_seconds,
                           unique_key,
                           
                    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                    WHERE trip_start_timestamp > '2017-01-01'
                          and trip_start_timestamp < '2017-07-01' 
                          and trip_seconds > 0 
                          and trip_miles > 0
                ) 
                SELECT hour_of_day,
                       COUNT(unique_key) as num_trips,
                       3600 * SUM(trip_miles) / sum(trip_seconds) as avg_mph
                FROM RelevantRides
                GROUP BY hour_of_day
                ORDER BY hour_of_day
                """
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**12)
speeds_query_job = client.query(speeds_query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
speeds_result = speeds_query_job.to_dataframe()

# Print the first five rows
speeds_result.head()
# View results
print(speeds_result)


