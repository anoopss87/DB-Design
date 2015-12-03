# DB-Design
Files and Indexing

Files and Indexing Programming Project
This program can be compiled in any jdk complier of version 1.8 and above.
Compilation Steps:
a)	Copy MyDatabase.java and input csv file to any directory as shown:
 
b)	Set the following path variables:
set path=%path%;C:\Program Files\Java\jdk1.8.0_60\bin;
c)	Compile the program using the following command:
Javac MyDatabase.java
d)	Then run the program using the following command:
Java –cp . MyDatabase
 
Now the user can give any command with the proper format. At this prompt, a blank enter will exit the program as shown:
 
The following commands are supported in this application:
1)	Import
The command should be of the form:
Import filename.csv 
Note: All strings are space separated i.e. there should be a single space between import and filename.csv. The semicolon in the end is optional
 
Binary file and index files are generated in the same directory.
2)	Insert
The command should be of the form:
Insert into table_name values (value1,value2,………….value11)
Note: All strings are space separated i.e. there should be a single space between insert and into and so on till the end. The semicolon in the end is optional
The format of the value list must be same as input file.
 
3)	Query
The command should be of the form:
Select * or attr_names separated by comma from table_name where field_name [NOT] op value
Note: The value field should be not be in double quotes even for string values as shown:
 

 
4)	Delete
The command should be of the form:
Delete from table_name where field_name [NOT] op value;
 

Note: 
a)	All strings are space separated i.e. there should be a single space between select and *, insert and into, select and *, delete and from and so on till the end of the query.
b)	The semicolon in the end is optional so as the NOT operator.
c)	The op can be of following types =, >, <, >= and <=. For Boolean attribute, only = is supported.
d)	[NOT] indicates NOT is optional. If NOT is used then there should be a space between NOT and op.
