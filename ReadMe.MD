BlazeDB
==========

Task 1 - Strategy for extracting WHERE clause
-----------------------------
I have created two methods to handle this:
	1. List<Expression> conditionForTable ( List<Expression> listExp , String table)
	2. List<Expression> conditionsForTwoTables (List<Expression> listExp , String tableOne , String tableTwo )

The WHERE conditions from the parsed SQL file will be stored in an Expression variable. 
I split the WHERE clause (which are joined with AND keyword) by passing it to a splitExpression method and store it in a list.
This list is then passed as arguments to both the methods mentioned above. 

1. conditionForTable() 
 - When I encounter a table (either in FROM clause or in JOIN clause), I pass the list of WHERE clauses and tableName to the conditionForTable method. 
 - In this method, I iterate through the list of WHERE clause expression using a for loop
 - Inside the loop, I extract the left and right expression of the current WHERE clause. 
 - Then these left and right expressions are checked if they contain the tableName (passed arg) for multiple scenarios 
   all of which determine if this expression is actually for this table or not. 
 - The scenarios include 1. both left and right exp refer to the same tuple (such as Student.A <op> Student.B)
                         2. if left exp is from a table and the right is an integer (such as Student.A <op> 3)
			 3. vice versa condition of 2 (such as (3 <op> Student.A)
                         4. both left and right are constants (such as 7 <op> 7)
 - If any of these cases match, then it means the WHERE clause is meant for this table.
   Hence I add it to a List<Expression> called returnClause
 - All this happens inside the for loop iterating through the list of WHERE expressions
 - Outside the loop, the List<Expression> is returned. In case there was no condition found that matches this table, null is returned. 
   Else, it will contain all the expression meant solely for this table/relation. 

2. conditionForTwoTables()
 - After I encounter a left and right table, I call this method to get all the conditions that relate to these tables. 
 - The same split list of WHERE clause and the names of both the tables are passed to this method
 - Inside this method, I iterate through the list of split WHERE clauses and check if the left table is a single table or a joined table.
 - For each expression, the left and right parts are extracted and compared with both the table names to see if they belong to them.  
 - If the left table is a result of join, 
   The scenarios checked are 1. left exp belongs to first table and right exp belongs to second table
			     2. left exp belongs to second table and right exp belongs to first table
 - In case the clause is reversed, (say first table is Student and second table is Course, then reversed clause is Course.X = Student.X) 
   then it is corrected after passing it through reorderClause() method.
 - If the clause matches one of the scenarios, then it means that clause is meant to join the tables which are passed here. 
   So I add it to the List of expressions to be returned

 - If the left table is not a result of previous join, then perform a similar computation to check if the passed expression belongs to the passed tables
 - Despite the similar computations, I have split this method into two using 'if-else' because of the way the schema and table names are represented. 
 - For joined tables, the first approach was easier to filter the conditions (only slight difference in the way I access table names and check)
 - Similar to the previous case, incase the conditions are reversed, then it is corrected by reorderClause() method. 
 - After checking each and every WHERE clause, it exits the loop
 - Lastly returns the list of WHERE clauses that are meant to join these two tables. In case nothing is present, null is returned. 


  
Task 2 - Optimisation
--------------------

I have tried optimising my code using the following methods	

1. Special case of SUM() queries
   If the queries involving only SUM() with just integer or product of integers and GROUPBY is null, then the actual data does not matter.
   Hence in SumOperator, I have handled it by having by checking if GROUPBY is null, there are no tableName.colName in SELECT clause. 
   If all these conditions are satisfied, then I assign a ScanOperator to each table, iterate through the table to findout the number of rows. 
   Then I multiply these together to get the total number of rows present in the result. 
   In case there is just one table present, it returns just the number of terms of that table.
   In case there are series of tables, it returns the number of rows of the cross join. 
   If the SUM() functions have parameters other than 1, then this i evaluated and then multiplied to the product obtained earlier. 
   All these are pushed into a tuple and stored as the output tuple. 

   This can greatly reduce the size of intermediate results by not calculating the entire join result (as data is irrelevant here)
   It just focuses on the size of the table, there by saving space and time. 

2. Combine multiple selections into a single SelectOperator if they reference the same table.
   I use conditionForTable method to identify all the WHERE clauses applicable for each table and pass it to be evaluated together.
   By doing so, the SelectOperator need not search for the clauses corresponding to every table it receives. 
   This reduces the number of operator nodes in the query plan. All the selections are applied to a table in a single go.

2. Combining multiple join conditions into a single joinOperator
   I use conditionsForTwoTables() method to gather all the join conditions into a single list for the tables being joined. 
   Since all the conditions are checked simultaneously in join, tuples will be joined only if all the conditions are satisfied. 
   There is just the final result produced eliminating any intermediate join tables.

4. Applying SelectOperator before each join
   Before passing tables to join, I apply SelectOperator to each table to reduce the number of tuples. 
   Doing so reduced the size of tables being joined which leads to faster processing.

5. Applying projection before join
   For queries that do not involve GROUPBY, I apply projection before joining so that only the necessary tuples are handed over to joinOperator. 
   This reduces the size of each tuple being checked during join. This leads to smaller intermediate results and faster processing.
   

