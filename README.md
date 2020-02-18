# **SQL Engine <Juvenile>**

A very Juvenile/rudimentary SQL engine built using python which parses and executes simple sql queries.

## **Repository Description**
* req_files directory contains the database and its schema.
* req_files/metadata,txt contains the schema of the database. Each tables schema is given between `<begin_table>` and `<end_table>`. The first line is the name of the table and the other lines are field(column) names.
* Data is stored as csv files in the directory of the same name as of the table. For example, the data of table 'table1' is given as 'table1.csv'.
* Main run code is in src/main.py and the relevant dependencies are in src/deps/sql_parser module.
* Currently only integer data is available.
* Queries are case insensitive.
* Only simple queries. Feature of nested queries to be implemented.
* Error handling available with sufficient debugging information.

#### **Run Query**
Go to src directory and then run the following command to execute the query
`python main.py "<query>"`

##### **Current Functionalities**
* Basic Select queries
* Aggregate functions
* Distinct Values
* Conditionals with at most two conditions allowed
* Join and Aliasing

### **Future Functionalities to be added:**
* Multi-Conditional ability (n number of conditionals to be made available)
* Sub-Query Functionality
* Show the parse tree with  relevant command (eg. _Show Tree query_)
* Ability to run multiple parallel queries.
* Add Query Optimizer intelligence
* Show the optimizer algorithm used with the relevant stats.
* Display the execution plan before the run starts