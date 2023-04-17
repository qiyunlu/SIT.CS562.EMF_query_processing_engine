# SIT.CS562.EMF_Query_Processing_Engine

Functional Description
----------------------
This project is to build a query processing engine for Ad-Hoc OLAP queries.<br>
Project reads in special formatted Ad-Hoc OLAP queries and generates a java program which can run independently to get the result.<br>
The query construct is based on an extended SQL syntax known as MF and EMF queries.<br>


***********************
Development Environment
-----------------------
Windows 10 Pro, 64-bit Operating System, x64-based processor<br>
Java(TM) SE Runtime Environment (build 1.8.0_161-b12)<br>
PostgreSQL 10.5, compiled by Visual C++ build 1800, 64-bit<br>
JDBC Driver Version 42.2.5


*****************
Project Structure
-----------------
-lib    // Libraries<br>
-src    // Source files<br>


**********
How To Run
----------
1. Download JDBC Driver corresponding to your java version and postgreSQL version, and put it under "lib" directory. (Link https://jdbc.postgresql.org/download.html)<br>
2. Open Windows Command Processor (not PowerShell), and go under "src" directory.<br>
3. Make sure there's no "Query.java" file. If you have, delete it.<br>
4. Write correct formatted operands to file "input", database information to "database". (Format mentioned later)<br>
5. To compile the project, run command:<br>
        javac -encoding utf-8 *.java<br>
6. To run the project, run command:<br>
        java -cp .;..\lib\postgresql-42.2.5.jar Kernel<br>
   *Command may different due to the different JDBC Driver or its path.<br>
7. File "Query.java" is generated under the same directory.<br>
   (To test the correctness of this project, you can use examples in "example" folder and follow the steps in "How To Test" section)<br>


***********
How To Test
-----------
1. Make sure you have the "Query.java" file by following the steps in "How To Run" section.<br>
2. To compile, run command:<br>
        javac -encoding utf-8 Query.java<br>
3. To run, run command:<br>
        java -cp .;..\lib\postgresql-42.2.5.jar Query<br>
   *Command may different due to the different JDBC Driver or its path.<br>
4. Compare the result shown in the Windows Command Processor with the result in the Data Output Bar of PostgreSQL.<br>


************
Input Format
------------
1. MF queries are transformed to EMF format.<br>
2. Must be 12 lines.<br>
3. All aggregates and attributes related to grouping variables need to be transformed. Like avg(x.quant) -> avg_1_quant, x.quant -> 1.quant.<br>
4. Grouping variable 0 always exists but will not be counted in the number of grouping variables.<br>
5. Odd lines are identification tags. Unchangeable.<br>
6. Even lines are operands and changeable. But each line has a different fixed format.<br>
-Line 2: Each attribute or aggregate is separated by a comma.<br>
-Line 4: One positive natural number.<br>
-Line 6: Each attribute is separated by a comma.<br>
-Line 8: Aggregates of the same grouping variable are put in one group and separated by commas. Each group is separated by a semicolon. Grouping variables are arranged from small to large. If one grouping variable has no aggregate, just insert a space to take a place.<br>
-Line 10: Such that clauses of the same grouping variable are put in one group. Each group is separated by a semicolon. Grouping variables are arranged from small to large. If one grouping variable has no aggregate, just insert a space to take a place.<br>
-Line 12: Insert the whole having condition. If there's no having condition, just insert a space to take a place.<br>

----------------------Example---------------------

// SELECT ATTRIBUTE(S):<br>
prod, month, count_3_quant<br>
// NUMBER OF GROUPING VARIABLES(n):<br>
3<br>
// GROUPING ATTRIBUTES(V):<br>
prod, month<br>
// F-VECT([F]):<br>
` `; avg_1_quant; avg_2_quant; count_3_quant<br>
// SELECT CONDITION-VECT([Sigma]):<br>
` `; 1.prod = prod and 1.month = month - 1; 2.prod = prod and 2.month = month + 1; 3.prod = prod and 3.month = month and 3.quant > avg_1_quant and 3.quant < avg_2_quant<br>
// HAVING CONDITION(G):<br>
` `<br>




***************
Database Format
---------------
1. Must be 8 lines.<br>
2. Odd lines are identification tags. Unchangeable.<br>
3. Even lines are information of your database and table. Changeable.<br>

----------------------Example---------------------

// database user<br>
postgres<br>
// database password<br>
26892681147<br>
// database url<br>
jdbc:postgresql://localhost:5432/postgres<br>
// table name<br>
sales<br>



*******
Version
-------
1.0<br>
Created on November 27, 2018<br>


**************
Developer List
--------------
Team: Eclipse Addict<br>
Member: Qiyun Lu<br>


*******************
Contact Information
-------------------
qiyunlu@outlook.com<br>
