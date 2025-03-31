BlazeDB
==========

Task 1 - Strategy for extracting WHERE clause
-----------------------------
I have created two methods to handle this:
	1. List<Expression> conditionForTable ( List<Expression> listExp , String table)
	2. List<Expression> conditionForTwoTables (List<Expression> listExp , String tableOne , String tableTwo )

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
 - For joined tables, the first approach was easier to filter the conditions (visually they slightly differ in the way I access table names and check)

 -  
 	


