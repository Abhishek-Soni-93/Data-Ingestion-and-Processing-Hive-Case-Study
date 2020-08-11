# Data-Ingestion-and-Processing-Hive-Case-Study/NYC Yellow Trip Data
The purpose of this dataset is to get a better understanding of the taxi system so that the city of New York can improve the efficiency of in-city commutes.

## Problem Statement
The New York City Taxi & Limousine Commission (TLC) has provided a dataset of trips made by the taxis in the New York City. The detailed trip-level data is more than just a vast list of taxi pickup and drop off coordinates.  

The records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations (location coordinates of the starting and ending points), trip distances, itemized fares, rate types, payment types, driver-reported passenger counts etc. The data used was collected and provided to the NYC Taxi and Limousine Commission (TLC) by technology providers authorized under the Taxicab & Livery Passenger Enhancement Programs (TPEP/LPEP).

The purpose of the dataset is to get a better understanding of the taxi system so that the city of New York can improve the efficiency of in-city commutes. Several exploratory questions can be asked about the travelling experience for passengers.

The dataset has been placed in the HDFS storage of the lab. The path to the data files is as follows: '/common_folder/nyc_taxi_data/'

#### Platform used:
https://www.corestack.io/corestack-claas/
Cloud Lab as a Service (CLaaS) is an on-demand, virtual practice environment for students/professionals pursuing technical subjects. This offering allows students & professionals the convenience of access to practice labs anytime and anywhere, to supplement classroom learning.

#### Expected Results:
##### Basic Data Quality Checks
- How many records has each TPEP provider provided? Write a query that summarises the number of records of each provider.
- The data provided is for months November and December only. Check whether the data is consistent, and if not, identify the data quality issues. Mention all data quality issues in comments.
- You might have encountered unusual or erroneous rows in the dataset. Can you conclude which vendor is doing a bad job in providing the records using different columns of the dataset?

###### IMPORTANT: Before partitioning any table, make sure you run the below commands.

###### SET hive.exec.max.dynamic.partitions=100000;
###### SET hive.exec.max.dynamic.partitions.pernode=100000;

#### Analysis-I

- Compare the overall average fare per trip for November and December.
- Explore the ‘number of passengers per trip’ - how many trips are made by each level of ‘Passenger_count’? Do most people travel solo or with other people?
- Which is the most preferred mode of payment?
- What is the average tip paid per trip? Compare the average tip with the 25th, 50th and 75th percentiles and comment whether the ‘average tip’ is a representative statistic (of the central tendency) of ‘tip amount paid’. Hint: You may use percentile_approx(DOUBLE col, p): Returns an approximate pth percentile of a numeric column (including floating point types) in the group.
- Explore the ‘Extra’ (charge) variable - what fraction of total trips have an extra charge is levied?

#### Analysis-II

- What is the correlation between the number of passengers on any given trip, and the tip paid per trip? Do multiple travellers tip more compared to solo travellers? Hint: Use CORR(Col_1, Col_2)
- Segregate the data into five segments of ‘tip paid’: [0-5), [5-10), [10-15) , [15-20) and >=20. Calculate the percentage share of each bucket (i.e. the fraction of trips falling in each bucket).
- Which month has a greater average ‘speed’ - November or December? Note that the variable ‘speed’ will have to be derived from other metrics. Hint: You have columns for distance and time.
- Analyse the average speed of the most happening days of the year, i.e. 31st December (New year’s eve) and 25th December (Christmas) and compare it with the overall average. 



