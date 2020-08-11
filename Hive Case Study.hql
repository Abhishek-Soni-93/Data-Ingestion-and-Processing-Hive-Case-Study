-- *************************** START OF THE CASE STUDY *****************************

-- Adding the required JAR file to the class path :
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar;

-- Defining the partition sizes: 
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

-- creating our workspace for casestudy:
Create database if not exists  assignment_ABhishekdb;
use  assignment_ABhishekdb;

-- *************************** LOADING THE DATA *****************************

-- Creating a table for the NEWYORK TLC Taxi dataset
-- Columns such as pickup_time and drop off time are imported as timestamps
-- As per the case study instruction using double for float values and int for integers as data types

--Creating External Table
CREATE EXTERNAL TABLE IF NOT EXISTS Nyc_Data_Taxi(
    VendorID int,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    Passenger_count int,
    Trip_distance double,
    RateCodeID int,
    Store_and_fwd_flag string,
    PULocationID int,
    DOLocationID int,
    Payment_type int,
    Fare_amount double,
    extra double,
    MTA_tax double,
    Tip_amount double,
    Tolls_amount double,
    Improvement_surcharge double,
    Total_amount double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/common_folder/nyc_taxi_data/'
TBLPROPERTIES ("skip.header.line.count"="2");

-- Visualising and understanding the data in table:
select * from Nyc_Data_Taxi limit 10;

--Querying to check total number of records in the dataset:
select count(*) from Nyc_Data_Taxi;
-- 1174568 records are there in this table


-- *************************** BASIC DATA QUALITY CHECKS *****************************

------------------------------------------------------------------------------------------------------------------------------
-- 1. How many records has each TPEP provider provided? Write a query that summarises the number of records of each provider.
------------------------------------------------------------------------------------------------------------------------------

-- If we refer the data dictionary :TPEP provider coresponds to vendor_id.
-- we have total 2 vendors : 
--      1. Creative Mobile Technologies
--      2. VeriFone Inc.

SELECT VendorID,COUNT(*) as No_of_records
FROM Nyc_Data_Taxi
GROUP BY VendorID
ORDER BY VendorID;
-- Conclusion:
-- Vendor 1 : Creative Mobile Technologies: 5,27,385 records
-- Vendor 2 : VeriFone Inc. : 6,47,183 records

--Lets see  what percentage of total data belongs to  Vendor 1 & Vendor 2:
select 647183/1174569;
-- Conclusion:
--55% of total trip data is from vendor 1 and rest 45% from vendor 2


------------------------------------------------------------------------------------------------------------------------------
-- 2. The data provided is for months November and December only. Check whether the data is consistent, and if not,  
-- identify the data quality issues. Mention all data quality issues in comments.
------------------------------------------------------------------------------------------------------------------------------

-- From the data dictionary 'tpep_pickup_datetime' corresponds to the date and time when the meter was engaged. 
-- And 'tpep_dropoff_datetime' corresponds to the date and time when the meter was disengaged.
-- These two are important features for determining the valid time which is only between 01 Nov 2017 till 31st Dec 2017.

-- Hence the invalid trip range will be where 'tpep_pickup_datetime'<01-Nov-2017 and 'tpep_pickup_datetime'>=01-Jan-2018 :
select  vendorid, count(*) from  Nyc_Data_Taxi 
where tpep_pickup_datetime < '2017-11-1 00:00:00.0' or tpep_pickup_datetime>='2018-01-01 00:00:00.0'
group by vendorid;
-- Conclusion:
-- Vendor 2 is having inconsistent data
-- 14 records outside the specified time range


-- Considering the drop time 'tpep_dropoff_datetime' now. The drop would have happended next day of last day i.e. 1st Jan 2018, 
-- so the invalid drop date would be >=02 Jan 2019:
select  vendorid, count(*) from  Nyc_Data_Taxi
where tpep_dropoff_datetime < '2017-11-1 00:00:00.0' or tpep_dropoff_datetime>='2018-01-02 00:00:00.0'
group by vendorid;
-- Conclusion:
-- Here also we can notice that vendor2 is having 6 inconsistent data and vendor 1 is having only 1.


select max(tpep_dropoff_datetime), max (tpep_pickup_datetime), min(tpep_pickup_datetime), min(tpep_dropoff_datetime)
from Nyc_Data_Taxi;
-- Conclusion:
-- Here we can see data needs cleansing as we have drop off dates in year 2019 and pickup dates from year 2003.

select vendorid, count(*) from Nyc_Data_Taxi
where tpep_dropoff_datetime<tpep_pickup_datetime
group by vendorid;
-- Conclusion:
-- For vendor 1 there are 73 records where the drop time is less than the pickup time, which is incorrect.

select vendorid, count(*) from Nyc_Data_Taxi
where tpep_dropoff_datetime=tpep_pickup_datetime
group by vendorid;
-- Conclusion:
-- We have 3419 records for vendor 1 and 3063 records for vendor 2 where pickup time and drop time are same. 
-- This might be valid data where the passenger or driver would have cancelled the ride and hence this data.
-- But since time is same, We will eliminate these records.


------------------------------------------------------------------------------------------------------------------------------
-- 3. You might have encountered unusual or erroneous rows in the dataset. Can you conclude which vendor is doing a bad job in
-- providing the records using different columns of the dataset? Summarise your conclusions based on every column where these 
-- errors are present. For example, There are unusual passenger count, i.e. 0 which is unusual. 
------------------------------------------------------------------------------------------------------------------------------

-- *************************** Examining the columns individually to check for errors ***************************

select passenger_count, count(*) as count_passenger
from  Nyc_Data_Taxi 
group by passenger_count
order by passenger_count;

-- Conclusion:

--0 	6824
--1 	827498
--2 	176872
--3 	50693
--4    	24951
--5 	54568
--6 	33146
--7 	12
--8 	3
--9 	1

-- This is a basic calculation where the passenger count >0 , otherwise there should not be a trip data.
-- Both Vendor 1 and 2 are seeding unusual passenger_count i.e equal to 0
-- vendor 1 has the most number of rows where the count =0
-- So we will eliminate such records during data cleaning.

select vendorid,passenger_count, count(*) 
from  Nyc_Data_Taxi 
where passenger_count in  (0,7,8,9) group by vendorid,passenger_count
order by passenger_count,vendorid;
-- Conclusion:
-- We can notice here the passenger count with more than 6 is for 15 records 
-- It can be a valid situation where car taken type is bigger car like SUV.
-- Or there might be kids in the car with family sitting on the parents laps. 
-- So , We decide keep this data as it is very small(only 15 records).

select  vendorid,count(*)
from  Nyc_Data_Taxi  
where passenger_count<=0 
group by vendorid;
-- Vendor 1 is having 6813 records and Vendor 2 is having just 11 records with less than or equal to zero passangers.


-- ******************* trip_distance:The elapsed trip distance in miles reported by the taximeter *******************

select min(trip_distance), max(trip_distance) from Nyc_Data_Taxi;
-- Conclusion:
-- minimun trip distace = 0 miles, maximum trip distance = 126.41 miles 
-- 126.41 miles can be valid trip distance considering the big size of New York city.

select  count(*) from  Nyc_Data_Taxi
where trip_distance<=0;
-- Conclusion:
-- There are 7402 records where trip distance is 0.

select 7402/1174568;
-- Conclusion:
-- We decide to ignore this data because zero or negative trip distance does not seems to be valid. 
-- This is just 0.006 percent of data.


select  vendorid,count(*)
from  Nyc_Data_Taxi
where trip_distance<=0 
group by vendorid;
-- Conclusion:
-- Vendor 1 is having 4217 records and vendor 2 is having 3185 records, 
-- So it is clear that Vendor 1 is provinding more incorrect data for trip distance.


-- ******************* RateCodeID: The final rate code in effect at the end of the trip *******************
-- The valid values here are from 1 to 6 as per the data dictionary.

select  
ratecodeid,count(*) 
from  Nyc_Data_Taxi 
group by ratecodeid
order by ratecodeid;
-- Conclusion:
-- There are such 9 records where invalid rate code id "99" exists. We will delete these records.

select vendorid , count(*) 
from  Nyc_Data_Taxi 
where ratecodeid=99
group by vendorid;
-- Conclusion:
-- More invalid data for rate id corresponds to the vendor 1.


-- ************* Store_and_fwd_flag: This flag indicates whether the trip record was held in vehicle memory  
-- before sending to the vendor, aka “store and forward,” because the vehicle did not have a connection to the server *************

select  
Store_and_fwd_flag,count(*) 
from  Nyc_Data_Taxi 
group by Store_and_fwd_flag
order by Store_and_fwd_flag;
-- Y = 3951, N = 1170617

select 3951/1174568;
-- Conclusion:
-- So we can notice 0.0033 percentage of data with flag as Y, this might be due to temporary network issue or trip 
-- where vehicle was not in good mobile network range, therefore we decide to keep this data.


-- ************** Payment_type : A numeric code signifying how the passenger paid for the trip ************** 
-- Valid values are between 1 to 6

select Payment_type, count(*) as count_of_passenger
from  Nyc_Data_Taxi   
group by Payment_type
order by Payment_type;
-- Conclusion:
-- This feature does not possess invalid records
-- The most preferred mode of paymet is 1 (credit card)


-- **************** Fare_amount: The time-and-distance fare calculated by the meter ****************

select max(fare_amount), min(fare_amount) from  Nyc_Data_Taxi;
-- Conclusion:
-- Max fare 650 seems to be valid fare(peak hours, surge in demand, long distance, premier cab etc) 
-- Negative fare is not correct these are invalid records, 


select count(*)
from  Nyc_Data_Taxi   
where fare_amount <0;
-- Conclusion:
-- We can notice here 558 negative records for the fare amount
-- So this can be due to fault meter reading and we decide to eliminate these records.

select vendorid ,count(*)
from  Nyc_Data_Taxi   
where fare_amount <0
group by vendorid;
-- Conclusion:
-- Here only Vendor 2 is responsible for negative fare amount.


-- ********** Extra: Miscellaneous extras and surcharges. Currently, 
-- this only includes the $0.50 and $1 rush hour and overnight charges. So the valid values are 0,0.5 and 1 only ********** 

select count(*)
from Nyc_Data_Taxi  
where extra not in(0,0.5,1);
-- Conclusion:
-- Here 4856 values can be ignored as these are incorrect data.

select vendorid ,count(*)
from Nyc_Data_Taxi  
where extra not in(0,0.5,1)
group by vendorid;
-- Conclusion:
-- Here Vendor 2 is providing more incorrect data(3033 records) and vendor 1 with less incorrect data(1823 records)


-- ***************** mta_tax : $0.50 MTA tax that is automatically triggered based on the metered rate in use *****************
select count(*)
from Nyc_Data_Taxi  
where mta_tax not in(0,0.5);
-- Conclusion:
-- Here 548 records are having incorrect values.

select vendorid ,count(*)
from Nyc_Data_Taxi
where mta_tax not in(0,0.5)
group by vendorid;
-- Conclusion:
-- Vendor 2 is majorly providing incorrect data for mta_tax with 547 out of 548 records.


-- *************** tip_amount :This field is automatically populated for credit card tips. Cash tips are not included ***************

select count(*)
from Nyc_Data_Taxi
where tip_amount<0;
-- Conclusion:
-- There are 4 records where tip amount is in negative 

select vendorid ,count(*)
from Nyc_Data_Taxi
where tip_amount<0
group by vendorid;
-- Conclusion:
-- All 4 incorrect records belongs to vendor 2 here. We will eliminate these records also.


-- *************** tolls_amount : Total amount of all tolls paid in trip ***************
select count(*)
from Nyc_Data_Taxi
where tolls_amount<0;
-- Conclusion:
-- Here only 3 records when toll amount is in negative 

select vendorid ,count(*)
from Nyc_Data_Taxi
where tolls_amount<0
group by vendorid;
-- Conclusion:
-- All 3 incorrect records belongs to vendor 2 here. We will eliminate these records also.


-- *************** improvement_surcharge : $0.30 improvement surcharge assessed trips at the flag drop. 
-- The improvement surcharge began being levied in 2015 ***************
 

select max(improvement_surcharge), min(improvement_surcharge) from Nyc_Data_Taxi;
-- Conclusion:
 -- maximum improvement surcharge =1 , minimun improvement_surcharge = -0.3
 
select count(*) 
from  Nyc_Data_Taxi
where improvement_surcharge not in (0,0.3);
-- Conclusion:
-- Total 562 invalid data records.

select vendorid ,count(*)
from Nyc_Data_Taxi
where improvement_surcharge not in (0,0.3)
group by vendorid;
-- Conclusion:
-- Again only vendor 2 is repsonsible for incorrect improvement_surcharge data.


-- *************** total_amount: The total amount charged to passengers. Does not include cash tips ***************

select min(total_amount), max(total_amount)
from Nyc_Data_Taxi;
-- Conclusion:
-- Max value seems ok 928 considering the surge in demand, premier rides etc , but min value is negative which is incorrect.


select count(*) from 
Nyc_Data_Taxi
where total_amount<0;
-- Conclusion:
-- Total 558 incorrect data records.

select vendorid,count(*) 
from Nyc_Data_Taxi
where total_amount<0 
group by vendorid;
-- Conclusion:
-- Again vendor 2 is only responsible for incorrect total amount of bill.


-- ********************** Bivariate Analyis of the columns/features **************************
-- Situation-1 : When passenger count is 1 , it can not be a group ride (ratecodeId=6) :

select vendorid,count(*) from 
Nyc_Data_Taxi
where passenger_count=1 
and ratecodeId= 6
group by vendorid;
-- Conclusion:
-- We have total 3 such invalid records and 2 belongs to vendor1 and 1 belong to vendor2.
-- We will remove such erronous data.

-- Situation-2 : Tip Amount is greater than total amount:

select vendorid,count(*) from 
Nyc_Data_Taxi
where tip_amount > total_amount
group by vendorid;
-- Conclusion:
-- We have total 558 such invalid records and belongs to vendor 2, this has asbe removed.

-- Situation-3 : Tip amount having payment type as Cash: 
-- As per data definition the cash tip is excluded for the tip amount , so mode of payment should not be cash when tip amount >0

select vendorid,count(*) from 
Nyc_Data_Taxi
where tip_amount > 0
and payment_type =2
group by vendorid;
-- Conclusion:
-- No rrecords so we are good

------------------------------------------------------------------------------------------------------------------------------
-- ************************************ Conclusion : Data Qauality Checks ************************************
------------------------------------------------------------------------------------------------------------------------------
-- We need to identify which vendor is contributing to more erronous data in the given dataset.

-- For following fields/columns Vendor1 (Creative Mobile Technologies) is providing higher incorrect data :
-- passenger_count
-- trip_distance
-- ratecodeid

-- On the other hand Vendor2(VeriFone Inc.) is providing  higher incorrect data for more number of columns:
-- Fare_amount
-- Extra
-- mta_tax
-- tip_amount
-- tolls_amount
-- improvement_surcharge
-- total_amount

-- CONCLUSION : Vendor2(VeriFone Inc.) needs to improvise and work on improving its billing systems to provide reliable data 
-- as most of erronous data for the billing related columns are from vendor 2.
-- On the flip side Vendor1 (Creative Mobile Technologies) needs to work with its taxi partners to record the data correctly 
-- for the passenger and their trips information.
-- Overall Vendor 2 is majorly contributing to incorrect data.


------------------------------------------------------------------------------------------------------------------------------
-- ********************************** CREATING A CLEAN, ORC PARTITIONED TABLE FOR ANALYSIS **********************************
------------------------------------------------------------------------------------------------------------------------------

--IMPORTANT: Before partitioning any table, make sure you run the below commands.
 
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

-- Drop table if already exists
DROP TABLE IF EXISTS Nyc_Data_Taxi_partitioned_orc;

-- As per the assignment instruction we will be using month and year for the partition only:
-- Creating table with required datatypes(columns), partition settings and compressd format configuration

Create external table if not exists Nyc_Data_Taxi_partitioned_orc(
    vendorid int,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count int,
    trip_distance double,
    RatecodeID int,
    store_and_fwd_flag string,
    PULocationID int,
    DOLocationID int,
    payment_type int,
    fare_amount double,
    extra double,
    mta_tax double,
    tip_amount double,
    tolls_amount double,
    improvement_surcharge double,
    total_amount double
)
partitioned by (yr int, mnth int)
stored as orc location '/user/hive/warehouse/avi.soni31_gmail'
tblproperties ("orc.compress"="SNAPPY");

-- Inserting the data in orc table with filter conditions:
insert overwrite table Nyc_Data_Taxi_partitioned_orc partition(yr,mnth)
select 
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
year(tpep_pickup_datetime) yr,
month(tpep_pickup_datetime) mnth
from  Nyc_Data_Taxi
where  (tpep_pickup_datetime >='2017-11-1 00:00:00.0' and tpep_pickup_datetime<'2018-01-01 00:00:00.0') and
(tpep_dropoff_datetime >= '2017-11-1 00:00:00.0' and tpep_dropoff_datetime<'2018-01-02 00:00:00.0') and
(tpep_dropoff_datetime>tpep_pickup_datetime) and
-- and YEAR(tpep_pickup_datetime)= 2017 and MONTH(tpep_pickup_datetime) in (11,12)
(passenger_count >0) and
(trip_distance>0) and 
(ratecodeid!=99) and
(fare_amount>0 ) and
 (extra in (0,0.5,1)) and
 (mta_tax  in (0,0.5)) and 
((tip_amount >=0 and Payment_type=1) or (Payment_type!=1 and tip_amount=0)) and
(tolls_amount >=0) and
(improvement_surcharge in (0,0.3)) and
(total_amount > tip_amount)and
(total_amount>0);


---Checking for data in table
SELECT * FROM Nyc_Data_Taxi_partitioned_orc LIMIT 10;

---Checking for total records available
SELECT COUNT(1) FROM Nyc_Data_Taxi_partitioned_orc;
-- 1153586

select 1174568-1153586;
-- 20982
select 20982/1174568;
-- 0.017 percentage of the data was removed post data cleaning (20982 records)


------------------------------------------------------------------------------------------------------------------------------
-- *************************************** ANALYSIS-1 ***************************************
------------------------------------------------------------------------------------------------------------------------------

-- 1.Compare the overall average fare per trip for November and December.

SELECT mnth, round(avg(fare_amount),2) as Average_fare_amount,round(avg(total_amount),2) as Avg_total_trip
FROM Nyc_Data_Taxi_partitioned_orc
GROUP BY mnth;

-- o/p  month      average_fare_amount       avg_total_trip
--      11	             12.91	                16.19
--   	12	             12.7	                15.89
-- Conclusion:
-- averge fare trip is more in november then december.
-- this may mean they are high taxs and surcharges in the november than in december.


-- 2.Explore the ‘number of passengers per trip’ - how many trips are made by each level of ‘Passenger_count’?
-- Do most people travel solo or with other people?

select Passenger_count, count(*) as Trip_count
from Nyc_Data_Taxi_partitioned_orc
group by Passenger_count;

-- Here 1153586 is total record count in cleaned table 
-- Below are the details of each level of passenger count:
-- passenger_count   trip_count
--      1	            817019
--	    2	            174783
--	    3	            50183
--	    4	            24680
--	    5	            54037
--	    6	            32882
--  	7	               3
-- Conclusion:
-- Its obvious from the above count that passengers would love to travel alone than in groups (almost 71 % solo travellers)
-- followed by trips having 2 passengers (15%).


-- 3.Which is the most preferred mode of payment?

select payment_type, count(*) as count_payment
from Nyc_Data_Taxi_partitioned_orc
group by payment_type;

-- o/p payment_type	count_payment
 
--	    1	            779153
--	    2	            368659
--      3           	4480
--  	4           	1295
-- Conclusion:
-- favourable mode of payment is credit card with 67.5% 
-- followed by Cash as second favourite most mode of payment with 32%.


--4.What is the average tip paid per trip? Compare the average tip with the 25th, 50th and 75th percentiles and 
--comment whether the ‘average tip’ is a representative statistic (of the central tendency) of ‘tip amount paid’. 
--Hint: You may use percentile_approx(DOUBLE col, p): Returns an approximate pth percentile of a numeric column 
--(including floating point types) in the group.

select round(avg(tip_amount),2) as avg_tip
from Nyc_Data_Taxi_partitioned_orc;

-- average tip -- 1.83

select percentile_approx(tip_amount,0.25) as 25th_percentile
,percentile_approx(tip_amount,0.5) as 50th_percentile
,percentile_approx(tip_amount,0.75) as 75th_percentile
,avg(tip_amount) as Avg_Tip_Amount
from Nyc_Data_Taxi_partitioned_orc;

-- o/p: 
--25th_percentile	50th_percentile	    75th_percentile	    avg_tip_amount
 	
--	0           	1.3596549479166669	    2.45	        1.825725376585753

-- Conclusion:
-- median -1.35
-- avg = 1.82 
-- Hence we can conclude from above that avg_tip is not the statistical representative(central tendency) of tip amount paid.

-- 5.Explore the ‘Extra’ (charge) variable - what fraction of total trips have an extra charge is levied?

select (count(case when Extra <> 0 then Extra end)/count(*)) *100
from Nyc_Data_Taxi_partitioned_orc;

-- about 46.15% of the total trips have levied extra charges.


------------------------------------------------------------------------------------------------------------------------------
-- *************************************** ANALYSIS-2 ***************************************
------------------------------------------------------------------------------------------------------------------------------

-- 1.What is the correlation between the number of passengers on any given trip, and the tip paid per trip?
-- Do multiple travellers tip more compared to solo travellers? 
-- Hint: Use CORR(Col_1, Col_2)

select corr(passenger_count, tip_amount)
from Nyc_Data_Taxi_partitioned_orc;
-- Conclusion:
-- The correlation between the passenger count and tip amount = -0.0053
-- negative correlation, seems like no connection b/w both


select round(corr(solo, tip_amount),2) from 
(select case when passenger_count=1 then 1 else 0 end solo,tip_amount 
from Nyc_Data_Taxi_partitioned_orc) x;

--o/p correaltion b/w passenger count =1 ans tip_amount is also low= 0.01
--lets compare with multiple travellers

select solo,round(avg(tip_amount),2) from 
(select case when passenger_count=1 then 1 else 0 end solo,tip_amount 
from Nyc_Data_Taxi_partitioned_orc ) x group by solo;

-- o/p 
--  0	1.8
--	1	1.84
-- Conclusion:
-- clearly there is no different values are approximately same.


-- 2.Segregate the data into five segments of ‘tip paid’: 
--[0-5), [5-10), [10-15) , [15-20) and >=20. Calculate the percentage share of each bucket 
--(i.e. the fraction of trips falling in each bucket).

select 
count(case when Tip_amount >= 0 and Tip_amount< 5 then Tip_amount end)*100.0/count(*) as  0_5
,count(case when Tip_amount >= 5 and Tip_amount< 10 then Tip_amount end)*100.0/count(*) as  5_10
,count(case when Tip_amount >= 10 and Tip_amount< 15 then Tip_amount end)*100.0/count(*) as  10_15
,count(case when Tip_amount >= 15 and Tip_amount< 20 then Tip_amount end)*100.0/count(*) as  15_20
,count(case when Tip_amount >= 20 then Tip_amount end)*100.0/count(*) as  G_T_20
from Nyc_Data_Taxi_partitioned_orc;

-- o/p

--	      0_5	               5_10	              10_15	              15_20	             G_T_20
--	92.40377397090464	5.637984510907726	1.6829261104070263	0.18724221687849887	0.08807319090210873
-- Conclusion:
-- Clearly bucket [0,5) has highest percentage of tip_paid.
-- around 92 percentage of tip comes from less than 5 dollars tip range.


-- 3.Which month has a greater average ‘speed’ - November or December? 
--Note that the variable ‘speed’ will have to be derived from other metrics. Hint: You have columns for distance and time.

select mnth , round(avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime) )/3600) ),2) avg_speed
from Nyc_Data_Taxi_partitioned_orc
group by mnth
order by avg_speed desc;

-- o/p    mnth      avg_speed
--         12	      11.07
--	       11         10.97
-- Conclusion:
-- Based on above data we have average speed of 10.97 miles/hour for November month and 11.07 miles/hour for December month.
-- Average speed of december comparitivly little high.. May be because of christmas ave.


-- 4. Analyse the average speed of the most happening days of the year, i.e. 31st December (New year’s eve) and 25th December (Christmas)
-- and compare it with the overall average. 

select round(avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime) )/3600) ),2) avg_speed
from Nyc_Data_Taxi_partitioned_orc;

--  over all average speed is 11.02

select from_unixtime(unix_timestamp(tpep_pickup_datetime), 'dd-MM-yyyy') as Happening_days, 
avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600)) as avg_speed
FROM Nyc_Data_Taxi_partitioned_orc
where mnth = 12
and day(tpep_pickup_datetime) in (25,31)
and year(tpep_dropoff_datetime) in (2017, 2018)
group by from_unixtime(unix_timestamp(tpep_pickup_datetime), 'dd-MM-yyyy');

--	happening_days	avg_speed
--	25-Dec-2017	    15.265472922267561
--	31-Dec-2017	    13.24443200187595

-- Conclusion :
-- Average speed is more on eve days when compared to overall average.
-- So if we compare the December Average speed which was 11.07 Mph is less than the avergae speed on christmans and New Year's eve. 
-- Amongst the Christmas and New Years eve, the average speed is higher on Christmas which is 15.26 Mph which is highest amongst all 3.
-- Average speed on Christmas is around 2 Mph higher than the New Years eve and 4.19 Mph higher than avergae December speed.


-- *************************** END OF THE CASE STUDY *****************************
