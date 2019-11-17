# Project Title: American Visitor Data Pipeline & Data Warehouse 
## Udacity Data Engineering Nanodegree Capstone Project
## 1. Project Summary


This Casptone Project is checking what I have learned about Data Pineline, Data Warehouse, Apache Airflow, Apache Spark and AWS. Combine with those knowledges to deliver a data pipeline for the program. This project will be an important part of my portfolio that will help me achieve the data engineering-related career goals. 


## 2 . The Project Steps:

####  Step 1: Scope the Project and Gather Data


- Project Scope: This project is based on Udaicty provided data sets. The main dataset includes data on immigration to the United State,  and supplementary datasets includes data on Airline companies. The Airflow data pinepine is transforming partitioned and cleaned data from AWS S3 to Redshift Data Warehouse for business intelligence. 
- Use Cases: 

> How many days in average each visitor stayed in US in April? 

> How many people visitor US in 2016 on a business visa? 

> Which Airline bring the most visitors in April?

> What are the top 3 US ports for visitors for 2016? 
  



- Data Source and Data Info: 

Data|File Format|Source |
---|---|---|
i94immigration|Parquet|Udacity Provided| 
I94_SAS_Labels_Descriptions.SAS | SAS|Udacity Provided  |
airline companies|CSV|[Data Source link](https://www.iata.org/about/members/Pages/airline-list.aspx?All=true) |

> I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.

> I94_SAS_Labels_Descriptions.SAS: This data comes with the i94immigration data as description file. 

> Airline companies: Current IATA member Airlines. This data has airline name, iata designator, 3-digital code, icao designator, country/territory. 


#### Step 2: Explore and Assess the Data

There are seven ETL files under elt_code folder to explore and transform all data files:



##### 2.1 Explore the Data 

- Drop unnecessary columns:

> drop_columns: count, dtadfile, visapost, entdepa, entdepd, entdepu, matflag, biryear, dtaddto,
                 insnum, admnum

- Missing Values: 

> Some columns of the i94immigration data have missing values, null values, ambigous values. There two types of missing values: integer and string. 

> Interger missing values replacement dictionary: {'cicid': -1, 'i94yr': -1, 'i94mon': -1, 'i94cit': 999, 'i94res': 239, 'i94mode': 9, 'arrdate': -1, 'depdate': -1, 'i94bir': -1, 'i94visa': -1}

> String missing values repleacement dictionary: {'i94port': 'XXX', 'i94addr': '99', 'visatype': 'unknown', 'occup': 'unknown', 'gender': 'U', 'airline': 'unknown', 'fltno': 'unknown'}
                     
- Duplicate Data: 

> Use Pyspark dropDuplicates() method to avoid unnessary rows. 



##### 2.2 File mapping


Input File name | ETL_code | ETL Process | Output File Name | size | Location|
---|---|---|---|---|---|
mode.txt|etl_mode1.py|Load from txt file, transform it into JSON file |1.mode.json |3 rows| s3://yulicapstone/lookups/1.mode.json|
location.txt |etl_location2.py |Load from txt file, transform it into JSON file  |2.location.json |289 rows|s3://yulicapstone/lookups/2.location.json|
port.txt |etl_port3.py |Load from txt file, transform it into JSON file  |3.port.json | 660 rows|s3://yulicapstone/lookups/3.port.json|
address.txt |etl_address4.py |Load from txt file, transform it into JSON file |4.address.json | 55 rows|s3://yulicapstone/lookups/4.address.json|
visa.txt |etl_visa5.py |Load from txt file, transform it into JSON file  |5.visa_category.json | 3 rows|s3://yulicapstone/lookups/5.visa_category.json|
airline.csv |etl_arline6.py |Load from CSV file, transform it into JSON file  |6.airline.json | 291 rows|s3://yulicapstone/lookups/6.airline.json|
immi94 SAS Parquet|etl_sas7.py |Load from parquet file, process missing/duplicate values, transform it into parquet files, partioned by year/month/day  |parquet files | 3096313 rows|s3://yulicapstone/i94immi_sas/year=YYYY/month=M/day=YYYYMMDD/partxxxxxx.qarquet|
 
##### 2.3 Convert timestamp

Convert arrdate and depdate to YYYYMMDD format. E.g.. 20160401


#### Step 3: Define the Data Model

##### 3.1 Star Schema and it's Advantages: 
- Star schemas are easy for end users and applications to understand and navigate. With a well-designed schema, users can quickly analyze large, multidimensional data sets. The main advantages of star schemas in a decision-support environment are:

- Query performance: Because a star schema database has a small number of tables and clear join paths, queries run faster than they do against an OLTP system. Small single-table queries, usually of dimension tables, are almost instantaneous. Large join queries that involve multiple tables take only seconds or minutes to run. In a star schema database design, the dimensions are linked only through the central fact table. When two dimension tables are used in a query, only one join path, intersecting the fact table, exists between those two tables. This design feature enforces accurate and consistent query results.

- Load performance and administration: Structural simplicity also reduces the time required to load large batches of data into a star schema database. By defining facts and dimensions and separating them into different tables, the impact of a load operation is reduced. Dimension tables can be populated once and occasionally refreshed. You can add new facts regularly and selectively by appending records to a fact table.

- Built-in referential integrity: A star schema has referential integrity built in when data is loaded. Referential integrity is enforced because each record in a dimension table has a unique primary key, and all keys in the fact tables are legitimate foreign keys drawn from the dimension tables. A record in the fact table that is not related correctly to a dimension cannot be given the correct key value to be retrieved.

- Easily understood: A star schema is easy to understand and navigate, with dimensions joined only through the fact table. These joins are more significant to the end user, because they represent the fundamental relationship between parts of the underlying business. Users can also browse dimension table attributes before constructing a query.

##### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model

Original File | ETL  | Table in DW | Info |
---|--|---|---|
i94 immigration SAS data files|Partitioned by Year/Month/Day and loaded into S3, e.g..S3://yulicastone/i94immi_data/year=2016/month=4/date=20160401|staging_i94immi||
i94 immigration SAS data files|Partitioned by Year/Month/Day and loaded into S3, e.g..S3://yulicastone/i94immi_data/year=2016/month=4/date=20160401|fact_visitor_event||
i94 immigration SAS data files|Partitioned by Year/Month/Day and loaded into S3, e.g..S3://yulicastone/i94immi_data/year=2016/month=4/date=20160401|dim_visitor||
i94 immigration SAS data files|Partitioned by Year/Month/Day and loaded into S3, e.g..S3://yulicastone/i94immi_data/year=2016/month=4/date=20160401|dim_time||
I94_SAS_Labels_Descriptions.SAS |Get mode.txt file, transform and save it into s3 as json |dim_mode ||
I94_SAS_Labels_Descriptions.SAS |Get location.txt file, transform and save it into s3 as json |dim_location ||
I94_SAS_Labels_Descriptions.SAS |Get port.txt file, transform and save it into s3 as json |dim_port ||
I94_SAS_Labels_Descriptions.SAS |Get address.txt file, transform and save it into s3 as json |dim_address ||
I94_SAS_Labels_Descriptions.SAS |Get visa.txt file, transform and save it into s3 as json |dim_visa ||
airline_company_lookup.csv |Use Spark to clean data, transform and save it into s3 as json |dim_airline ||







#### Step 4: Run Pipelines to Model the Data


#####  4.1 Build the data pipeline to build the data model
- 1.The Data Model:  

> ![Capstone](https://raw.githubusercontent.com/yfocusy/DataEngineering-Udacity-Nanodegree/master/capstone_db.png "Capstone Data Model")

- 2.The Data Pineline Dag in Airflow: the Graph View of building the data pipelines to create the data model.

> ![Dag](https://raw.githubusercontent.com/yfocusy/DataEngineering-Udacity-Nanodegree/master/dag.png "Graph view")

- 3.The Data Pineline Dag in Airflow: the Tree View

> ![Capstone](https://raw.githubusercontent.com/yfocusy/DataEngineering-Udacity-Nanodegree/master/airflow.png "Tree view")






##### 4.2 Data Quality Checks

- 1. All SAS parquet files passed to data quality check is 3 million rows
>


- 2. Dim tables: As 6 of 8 dimentional tables are working as lookup tables. Before insert data, checking the row number is nessary to avoidng missing data. 

- 3. Check row numbers after data backfiled.


#### Step 5: Project Summary

##### 5.1 Choice of tools and technologies for the project

> Language: Python, PostgreSQL

> Data Modeling: Star Schema

> ETL: Pandas, Pyspark

> Data Warehouse: AWS Redshift

> Data Pinelines: Apache Airflow

> File Format: txt, csv, json, parquet

> Storage: AWS S3


##### 5.2 Propose how often the data should be updated and why

> The visitors in each port arrive in and departure US at anytime. As in the i94immigration files, time limit can be precise to year, month, day, can't be precise the minute or seconds. It would be efficient to be upated daily based on current data. 

##### 5.3 Write a description of how you would approach the problem differently under the following scenarios:
- 1. The data was increased by 100x.

> Partition for Airflow: Currently, i94immigration parquet files are partitioned by year/month/day in the S3 storage (e.g.. s3://yulicastone/i94immi_data/year=2016/month=4/date=20160401/part.xxxxxx.parquet). The amount of data loaed in data pineline for each day has been reduced. If the data increased by 100x, I would like to increase location as another partition parameter to fast the transformation in data pipelines. 

> Data Warehouse: People can run analytic queries against petabytes of data stored locally in Redshift, and directly against exabytes of data stored in Amazon S3. Amazon Redshift's MPP architecture means I can increase the performance by increasing the number of nodes in the data warehouse cluster. The optimal amount of data per compute node depends on the application characteristics and the query performance needs. An Amazon Redshift data warehouse cluster can contain from 1-128 compute nodes, depending on the node type.



- 2. The data populates a dashboard that must be updated on a daily basis by 7am every day.

> The Airflow scheduler monitors all tasks and all DAGs, and triggers the task instances whose dependencies have been met. Apache Airflow has a feature to process the dag daily at 00:00:00. Before 7am the data warehouse should be updated for the dashboard. 


- 3. The database needed to be accessed by 100+ people.

> Access to Amazon Redshift requires credentials that AWS can use to authenticate your requests. Those credentials must have permissions to access AWS resources, such as an Amazon Redshift cluster. Different people can have valid credentials to authenticate the requests, but unless people have permissions they cannot create or access Amazon Redshift resources.

> Storing data efficiently is critical for performance. Efficient data storage in Redshift allows for faster scanning. It also results in more efficient IO, which is paramount when working with large datasets.



