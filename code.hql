DataEngineering_Assessment using Apache Hive

Step1: Remove '|D|'  and '|H|' from design file.csv before loading in hdfs location

Step2:In Hive Terminal
      CREATING HADOOP DIRECTORY AND COPYING .csv FILE IN IT​    
      ->hdfs dfs –mkdir Design_file
​​      ->hdfs dfs -copyFromLocal <local file location> <hdfs location of Design file directory>

Step 2: CREATING SCHEMA IN HIVE TERMINAL​

CREATE SCHEMA IF NOT EXISTS Patients;​--create database
USE Patients;--use database
--creating stage/intermediate table
CREATE TABLE Staging_table(
Name VARCHAR(255) NOT NULL PRIMARY KEY,
Cust_I VARCHAR(18) NOT NULL,
Open_Dt DATE NOT NULL,
Consult_Dt DATE,
VAC_ID CHAR(10),
DR_Name CHAR(10),
State_C CHAR(10),
Country CHAR(10),
DOB DATE,
FLAG CHAR(10)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘|​
LINES TERMINATED BY ‘\n’
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

Loading data into Staging_table:-
LOAD DATA INPATH ‘<Hadoop filepath containing design_file.csv>’​
OVERWRITE INTO TABLE Staging_table;

Checking if data is loaded correctly:-
SELECT * FROM Staging_table limit 5; 

Now,Similarly creating country-wise tables for final data:
Example: Table_India, Table_USA, Table_China etc.

For final data population(Example for 1 country India):
INSERT INTO table Table_India
SELECT 
Name as Customer_Name,
Cust_I as Customer_Id,
Open_Dt as Open_Date,
Consul_Dt as Last_Consulted_Date,
VAC_ID as Vaccination_Id, 
DR_Name,
State_C,
Country,
DOB,
FLAG as  Is_Active
FROM Staging_table
WHERE Country='IND'
;

