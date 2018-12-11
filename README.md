# SIT.CS562.EMF_Query_Processing_Engine

Functional Description
----------------------
This project is to build a query processing engine for Ad-Hoc OLAP queries.
Project reads in special formatted Ad-Hoc OLAP queries and generates a java program which can run independently to get the result.
The query construct is based on an extended SQL syntax known as MF and EMF queries.


***********************
Development Environment
-----------------------
Windows 10 Pro, 64-bit Operating System, x64-based processor
Java(TM) SE Runtime Environment (build 1.8.0_161-b12)
PostgreSQL 10.5, compiled by Visual C++ build 1800, 64-bit
JDBC Driver Version 42.2.5


*****************
Project Structure
-----------------
-lib    // Libraries
-src    // Source files


**********
How To Run
----------
1. Download JDBC Driver corresponding to your java version and postgreSQL version, and put it under "lib" directory. (Link https://jdbc.postgresql.org/download.html)
2. Open Windows Command Processor (not PowerShell), and go under "src" directory.
3. Make sure there's no "Query.java" file. If you have, delete it.
4. Write correct formatted operands to file "input", database information to "database".
   (Format mentioned later)
5. To compile the project, run command:
        javac -encoding utf-8 *.java
6. To run the project, run command:
        java -cp .;..\lib\postgresql-42.2.5.jar Kernel
   *Command may different due to the different JDBC Driver or its path.
7. File "Query.java" is generated under the same directory.
   (To test the correctness of this project, you can use examples in "example" folder and follow the steps in "How To Test" section)


***********
How To Test
-----------
1. Make sure you have the "Query.java" file by following the steps in "How To Run" section.
2. To compile, run command:
        javac -encoding utf-8 Query.java
3. To run, run command:
        java -cp .;..\lib\postgresql-42.2.5.jar Query
   *Command may different due to the different JDBC Driver or its path.
4. Compare the result shown in the Windows Command Processor with the result in the Data Output Bar of PostgreSQL.


************
Input Format
------------
1. MF queries are transformed to EMF format.
2. Must be 12 lines.
3. All aggregates and attributes related to grouping variables need to be transformed. Like avg(x.quant) -> avg_1_quant, x.quant -> 1.quant.
4. Grouping variable 0 always exists but will not be counted in the number of grouping variables.
5. Odd lines are identification tags. Unchangeable.
6. Even lines are operands and changeable. But each line has a different fixed format.
-Line 2: Each attribute or aggregate is separated by a comma.
-Line 4: One positive natural number.
-Line 6: Each attribute is separated by a comma.
-Line 8: Aggregates of the same grouping variable are put in one group and separated by commas. Each group is separated by a semicolon. Grouping variables are arranged from small to large. If one grouping variable has no aggregate, just insert a space to take a place.
-Line 10: Such that clauses of the same grouping variable are put in one group. Each group is separated by a semicolon. Grouping variables are arranged from small to large. If one grouping variable has no aggregate, just insert a space to take a place.
-Line 12: Insert the whole having condition. If there's no having condition, just insert a space to take a place.

----------------------Example---------------------
// SELECT ATTRIBUTE(S):

prod, month, count_3_quant

// NUMBER OF GROUPING VARIABLES(n):

3

// GROUPING ATTRIBUTES(V):

prod, month

// F-VECT([F]):

 ; avg_1_quant; avg_2_quant; count_3_quant

// SELECT CONDITION-VECT([Sigma]):

 ; 1.prod = prod and 1.month = month - 1; 2.prod = prod and 2.month = month + 1; 3.prod = prod and 3.month = month and 3.quant > avg_1_quant and 3.quant < avg_2_quant

// HAVING CONDITION(G):
 




***************
Database Format
---------------
1. Must be 8 lines.
2. Odd lines are identification tags. Unchangeable.
3. Even lines are information of your database and table. Changeable.

----------------------Example---------------------
// database user

postgres

// database password

26892681147

// database url

jdbc:postgresql://localhost:5432/postgres

// table name

sales



*******
Version
-------
1.0
Created on November 27, 2018


**************
Developer List
--------------
Team: Eclipse Addict
Member: Qiyun Lu


*******************
Contact Information
-------------------
qlu5@stevens.edu
