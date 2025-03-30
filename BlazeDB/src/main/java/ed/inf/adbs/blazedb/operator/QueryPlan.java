package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

/*
 * This is the class which build the query plan and passes it for execution.
 * It checks all the variables containing the parsed SQL queries and decides which operator to be called. 
 * The buildQueryPlan method is divided into two - One which handles all queries involving JOIN, another that handles queries with no JOIN
 * 
 */
public class QueryPlan {

	/*
	 * Constructor method that takes all the parsed SQL query variables and calls
	 * the necessary operators to build tree of operators Two main parts are
	 * present. One that handles JOIN clause and one that does not.
	 * 
	 * 
	 * @param SELECT The list of SelectItems that mentions all the attributes that
	 * needs to be in the output
	 * 
	 * @param DISTINCT The variable that mentions if duplicates need to be
	 * eliminated in the output
	 * 
	 * @param ORDERBY The list of OrderByElements which specifies the attributes,
	 * using which the tuples need to be sorted.
	 * 
	 * @param GROUPBY The ExpressionList that mentions the attributes that need to
	 * be grouped together to produce the output.
	 * 
	 * @param WHERE It is the Expression that contains all the WHERE clauses that
	 * the tuples need to satisfy to be printed.
	 * 
	 * @param JOIN It consists of all the tables that needs to be joined to the
	 * first table. It will be null if only one table is specified in input SQL
	 * file.
	 * 
	 * @param FROM The name of the first table that needs to be scanned.
	 * 
	 * @return (Operator) The root of the execution tree to be returned. it is of
	 * they type operator.
	 */
	public static Operator buildQueryPlan(List<SelectItem<?>> SELECT, Distinct DISTINCT, List<OrderByElement> ORDERBY,
			ExpressionList GROUPBY, Expression WHERE, List<Join> JOIN, FromItem FROM) {

		DatabaseCatalog catalog = DatabaseCatalog.getInstance();

		if (SELECT.toString().toLowerCase().contains("sum") && checkForTables(SELECT, JOIN, FROM) && GROUPBY==null) {
			List<String> combinedTableNames = getTableForSum(FROM, JOIN);
			Operator root = new CountRowsOperator(SELECT, combinedTableNames);
			return root;
		}

		if (JOIN == null) {

			// This ensures that there is only 1 table because FromItem picks up only one
			// table
			Operator root = new ScanOperator(FROM.toString());
			Map<String, Integer> attributeHashIndex = root.getAttributeHashIndex();

			if (WHERE != null) {
				root = new SelectionOperator(root, WHERE, attributeHashIndex);
			}

			// Sorting can be done here as well but it leads to larger tuples being compared
			// which can reduce the speed
			if (!SELECT.toString().contains("[*]")) {

				if (!SELECT.toString().toLowerCase().contains("sum") && GROUPBY == null) {
					// Normal column projection
					root = new ProjectionOperator(root, SELECT, attributeHashIndex);
					// Pulling the new attribute hash index containing the projected columns only
					attributeHashIndex = root.getAttributeHashIndex();
				}

				else { // Handles the case where SUM and/or GROUPBY is present
					root = new SumOperator(root, GROUPBY, SELECT, attributeHashIndex);
					attributeHashIndex = root.getAttributeHashIndex();
				}

			}
			if (DISTINCT != null) {
				root = new DuplicateEliminationOperator(root);
			}
			if (ORDERBY != null) {
				root = new SortOperator(root, ORDERBY, attributeHashIndex);
			}
			return root;

		} // end of if(JOIN==null) block

		if (JOIN != null) {

			Operator leftChild = new ScanOperator(FROM.toString());
			Map<String, Integer> attributeHashIndex_lChild = leftChild.getAttributeHashIndex();
			String leftTableName = FROM.toString();

			// split the where clause
			List<Expression> listExp = splitExpression(WHERE);
			if (!(WHERE == null)) {

				// This checking is just for the first table.
				// For all other subsequent tables, it happens inside the following loop.
				List<Expression> listTableOneClause = conditionForTable(listExp, leftTableName);
				if (!listTableOneClause.isEmpty()) {
					Expression tableOneClause = combineWithAnd(listTableOneClause);
					leftChild = new SelectionOperator(leftChild, tableOneClause, attributeHashIndex_lChild);
				}
			}

			//*//
			// This cannot be applied to GROUPBY because there may be some attributes that
			// are necessary for grouping but not present in select.
			if (!SELECT.toString().contains("[*]")) {
				if (!SELECT.toString().toLowerCase().contains("sum") && GROUPBY == null) {
					// Handling projection of column attributes only.
					leftChild = new ProjectionOperator(leftChild, SELECT, attributeHashIndex_lChild);
					// Pulling the new attribute hash index(schema) containing the projected columns
					// only

					attributeHashIndex_lChild = leftChild.getAttributeHashIndex();
				}
			}
			
			//*//
			for (Join join : JOIN) {
				// this loop is to iteratively handle all the tables in JOIN
				Operator rightChild = new ScanOperator(join.getRightItem().toString());
				Map<String, Integer> attributeHashIndex_rChild = rightChild.getAttributeHashIndex();

			//*//
				// This cannot be applied to GROUPBY because there may be some attributes that
				// are necessary for grouping but not present in select.
				if (!SELECT.toString().contains("[*]")) {
					if (!SELECT.toString().toLowerCase().contains("sum") && GROUPBY == null) {
						// Handling projection of column attributes only.
						rightChild = new ProjectionOperator(rightChild, SELECT, attributeHashIndex_rChild);
						// Pulling the new attribute hash index(schema) containing the projected columns
						// only
						attributeHashIndex_rChild = rightChild.getAttributeHashIndex();
					}
				}
				//*//		

				if (!(WHERE == null)) {

					List<Expression> listTableTwoClause = conditionForTable(listExp, join.toString());
					if (!listTableTwoClause.isEmpty()) {
						Expression tableTwoClause = combineWithAnd(listTableTwoClause);
						rightChild = new SelectionOperator(rightChild, tableTwoClause, attributeHashIndex_rChild);
					}

					List<Expression> listTablesJoinClause = conditionsForTwoTables(listExp, leftTableName,
							join.toString());
					if (!listTablesJoinClause.isEmpty()) {
						// If there is a condition for these two tables, pass it to join operator
						leftChild = new JoinOperator(leftChild, rightChild, listTablesJoinClause,
								attributeHashIndex_lChild, attributeHashIndex_rChild);
					} else {
						// If there is something in WHERE clause that is not applicable to these two
						// tables, simply compute the cross product
						leftChild = new JoinOperator(leftChild, rightChild);
					}
				} else {
					// Since WHERE clause is null, just perform a cross join of the two tables
					leftChild = new JoinOperator(leftChild, rightChild);
				}

				int offset = attributeHashIndex_lChild.size();

				// Combining the schemas to ensure that for any subsequent joins, the updated
				// schema is used
				for (Map.Entry<String, Integer> entry : attributeHashIndex_rChild.entrySet()) {
					if (!attributeHashIndex_lChild.containsKey(entry.getKey()))
						attributeHashIndex_lChild.put(entry.getKey(), entry.getValue() + offset);
				}
				leftTableName = leftTableName.concat(" join " + join.toString());

			} // End of for( Join join : JOIN ) block

			// Sorting can be done here as well but it leads to larger tuples being compared
			// which can reduce the speed
			if (!SELECT.toString().contains("[*]")) {
				if (!SELECT.toString().toLowerCase().contains("sum") && GROUPBY == null) {
					// Handling projection of column attributes only. No SUM() and No GROUPBY
					leftChild = new ProjectionOperator(leftChild, SELECT, attributeHashIndex_lChild);
					// Pulling the new attribute hash index(schema) containing the projected columns
					// only
					attributeHashIndex_lChild = leftChild.getAttributeHashIndex();
				} else { // Presence of SUM and/or GROUPBY
					leftChild = new SumOperator(leftChild, GROUPBY, SELECT, attributeHashIndex_lChild);
					// Pulling the new attribute hash index(Schema) containing the projected columns
					// only
					attributeHashIndex_lChild = leftChild.getAttributeHashIndex();
				}

			}

			if (DISTINCT != null) {
				leftChild = new DuplicateEliminationOperator(leftChild);
			}

			if (ORDERBY != null) {
				leftChild = new SortOperator(leftChild, ORDERBY, attributeHashIndex_lChild);
			}
			return leftChild;

		}
		return null;

	}

	/*
	 * This method is for checking if the WHERE conditions passed is applicable to
	 * the tables that is passed This method takes two tables and a list of
	 * expression.
	 * 
	 * @param listExp This is a list of Expression consisting of WHERE clauses
	 * 
	 * @param tableOne The name(String) of the first table to be joined
	 * 
	 * @param tableTwo The name(String) of the second table to be joined
	 * 
	 * @return returnClause The List of expression containing the correct order of
	 * all the WHERE clauses applicable to these two tables.
	 */
	private static List<Expression> conditionsForTwoTables(List<Expression> listExp, String tableOne, String tableTwo) {

		List<Expression> returnClause = new ArrayList<>();

		for (Expression exp : listExp) {
			ComparisonOperator e = (ComparisonOperator) exp;

			if (tableOne.contains("join")) {
				// The left table is a result of a previous JOIN operation

				String[] leftTableInJoinClause = e.getLeftExpression().toString().toLowerCase().split("\\.");
				String[] rightTableInJoinClause = e.getRightExpression().toString().toLowerCase().split("\\.");

				// Index 0 will be the name of the table that we wish to check
				// Checking for commutativity of WHERE clause. Student <op> Course and Course
				// <op> Student
				if ((tableOne.toLowerCase().contains(leftTableInJoinClause[0].toLowerCase())
						&& tableTwo.toLowerCase().contains(rightTableInJoinClause[0].toLowerCase()))
						|| (tableOne.toLowerCase().contains(rightTableInJoinClause[0].toLowerCase())
								&& tableTwo.toLowerCase().contains(leftTableInJoinClause[0].toLowerCase()))) {
					if ((tableOne.toLowerCase().contains(leftTableInJoinClause[0].toLowerCase())
							&& tableTwo.toLowerCase().contains(rightTableInJoinClause[0].toLowerCase()))) {
						// The order of WHERE clause is in the order of table specified. Hence add it as
						// it is.
						returnClause.add(e);
					} else if ((tableOne.toLowerCase().contains(rightTableInJoinClause[0].toLowerCase())
							&& tableTwo.toLowerCase().contains(leftTableInJoinClause[0].toLowerCase()))) {
						// Handling the case where the Tables and Join Conditions are in reverse order
						// For example: SELECT * FROM Student, Enrolled WHERE Enrolled.A = Student.A;
						// Hence reverse the WHERE conditions and add it to the list of conditions
						returnClause.add(reorderClause(e));
					}
				}

			} else {
				// The left table is not a result of previous join operation
				if ( // Checking for the join condition commutativity
				(e.getLeftExpression().toString().toLowerCase().contains(tableOne.toLowerCase())
						&& e.getRightExpression().toString().toLowerCase().contains(tableTwo.toLowerCase()))
						|| (e.getLeftExpression().toString().toLowerCase().contains(tableTwo.toLowerCase())
								&& e.getRightExpression().toString().toLowerCase().contains(tableOne.toLowerCase()))) {
					if (e.getLeftExpression().toString().toLowerCase().contains(tableOne.toLowerCase())
							&& e.getRightExpression().toString().toLowerCase().contains(tableTwo.toLowerCase())) { // Order
																													// of
																													// join
																													// attributes
																													// same
																													// as
																													// order
																													// of
																													// tables
						returnClause.add(e);
					} else if (e.getLeftExpression().toString().toLowerCase().contains(tableTwo.toLowerCase())
							&& e.getRightExpression().toString().toLowerCase().contains(tableOne.toLowerCase())) {
						// Split the clause. Reorder it and add it to list
						returnClause.add(reorderClause(e));
					}
				}
			}
		}
		return returnClause;
	}

	/*
	 * Method to reorder the Expressions in the WHERE clause. There can be a case
	 * where the order of tables and their WHERE conditions specified are
	 * interchanged. This method helps to swap the order of Expressions in the WHERE
	 * clause.
	 * 
	 * @param e The WHERE clause in the form of ComparisonOperator. Consists of a
	 * leftExpression and a rightExpression joined by an Operator
	 * 
	 * @return reversedClause The WHERE clause in the form of ComparisonOperator
	 * with correct order of Expression. (same as the order of tables)
	 */
	private static ComparisonOperator reorderClause(ComparisonOperator e) {
		ComparisonOperator reversedClause = null;

		Expression leftExp = e.getLeftExpression();
		Expression rightExp = e.getRightExpression();
		ComparisonOperator op = null;

		if (e instanceof GreaterThan) {
			op = new MinorThan();
		}
		if (e instanceof MinorThan) {
			op = new GreaterThan();
		}
		if (e instanceof GreaterThanEquals) {
			op = new MinorThanEquals();
		}
		if (e instanceof MinorThanEquals) {
			op = new GreaterThanEquals();
		}
		if (e instanceof EqualsTo) {
			op = new EqualsTo();
		}
		if (e instanceof NotEqualsTo) {
			op = new NotEqualsTo();
		}

		op.setLeftExpression(rightExp);
		op.setRightExpression(leftExp);
		reversedClause = op;

		return reversedClause;
	}

	/*
	 * This method is for checking if the WHERE conditions passed is applicable to
	 * the table that is passed This method takes one tables and entire list of
	 * expression.
	 * 
	 * @param listExp This is a list of Expression consisting of WHERE clauses
	 * 
	 * @param table The name(String) of the table to be checked
	 * 
	 * @return returnClause The List of expression containing all the WHERE clauses
	 * applicable to the passed table
	 */
	private static List<Expression> conditionForTable(List<Expression> listExp, String table) {

		List<Expression> returnClause = new ArrayList<>();

		for (Expression exp : listExp) {

			ComparisonOperator e = (ComparisonOperator) exp;

			if // This checks if the where clause for the table is a single table where clause
				// or not
			((e.getLeftExpression().toString().toLowerCase().contains(table.toLowerCase())
					&& e.getRightExpression().toString().toLowerCase().contains(table.toLowerCase()))
					|| (e.getLeftExpression().toString().toLowerCase().contains(table.toLowerCase())
							&& !(e.getRightExpression() instanceof Column))
					|| (e.getRightExpression().toString().toLowerCase().contains(table.toLowerCase())
							&& !(e.getLeftExpression() instanceof Column))
					|| (isInteger(e.getLeftExpression()) && isInteger(e.getRightExpression()))) {
				returnClause.add(e);
			}
		}
		return returnClause;
	}

	/*
	 * Method to check if the passed Expression is of the type Integer or not
	 * 
	 * @param exp The Expression read from the WHERE clause.
	 * 
	 * @return (boolean) The result of checking if the passed parameter is an
	 * integer or not,
	 */
	public static boolean isInteger(Expression exp) {
		try {
			Integer.parseInt(exp.toString());
			return true;

		} catch (NumberFormatException e) {
			return false;
		}
	}

	/*
	 * Method to recursively split the given Expression into a list of expression.
	 * Since we are using only AND operator to join the Expressions, this method can
	 * be used. In case there is OR used to join the operators, then this approach
	 * of dealing with queries might not be optimal.
	 * 
	 * @param expression The Expression containing the entire WHERE clause parsed
	 * from the SQL input file.
	 * 
	 * @return expressionList This is a list of Expressions that is obtained after
	 * splitting all the passed Expression
	 */
	private static List<Expression> splitExpression(Expression expression) {
		List<Expression> expressionList = new ArrayList<>();

		// if the expression is a logical conjunction and/or then split it recursively
		if (expression instanceof AndExpression) {
			AndExpression andExpression = (AndExpression) expression;

			expressionList.addAll(splitExpression(andExpression.getLeftExpression()));
			expressionList.addAll(splitExpression(andExpression.getRightExpression()));
		}
		// The below wont be of much use to this program but added anyway to test custom
		// queries.
		else if (expression instanceof OrExpression) {
			OrExpression orExpression = (OrExpression) expression;

			expressionList.addAll(splitExpression(orExpression.getLeftExpression()));
			expressionList.addAll(splitExpression(orExpression.getRightExpression()));
		} else {
			// Base case: simple expression
			expressionList.add(expression);
		}
		return expressionList;
	}

	/*
	 * Method to combine the list of expressions back to a single Expression using
	 * AND keyword. I understand this method and previous method are contradicting
	 * to each other but I used this approach to make better computations during
	 * query evaluation.
	 * 
	 * @param expressions The list of expressions to be joined using AND keyword
	 * 
	 * @return combinedExpression The single Expression that contains all the
	 * expressions joined together.
	 */
	private static Expression combineWithAnd(List<Expression> expressions) {
		if (expressions == null || expressions.isEmpty()) {
			return null; // No expressions to combine
		}
		// Start with the first expression
		Expression combinedExpression = expressions.get(0);

		// Iterate through the list and combine the expressions with AND
		for (int i = 1; i < expressions.size(); i++) {
			combinedExpression = new AndExpression(combinedExpression, expressions.get(i));
		}

		return combinedExpression;
	}

	/*
	 * Method to check if there is any instance of a table mentioned in the Select
	 * clause. It is used to filter queries that contain only SUM( integer(s) )
	 * functions. In case there is a tableName.columnName inside the SUM() function,
	 * it returns false. Else true.
	 * 
	 * @param SELECT It is a list of selectItems is expected to be printed in the
	 * output file
	 * 
	 * @param JOIN It is a list of all the tables that needs to be joined to the
	 * table mentioned in the FROM variable
	 * 
	 * @param FROM It is the name of the first table in the input SQL file
	 * 
	 * @return (boolean) The result after checking if the mentioned tables'
	 * attributes are present in the select clause or not
	 */
	private static boolean checkForTables(List<SelectItem<?>> SELECT, List<Join> JOIN, FromItem FROM) {
		if (SELECT.toString().toLowerCase().contains(FROM.toString().toLowerCase()))
			return false;
		if (!(JOIN == null)) {
			for (Join join : JOIN) {
				if (SELECT.toString().toLowerCase().contains(join.toString().toLowerCase()))
					return false;
			}
		}

		return true;
	}

	/*
	 * Method that adds the names of tables mentioned in the FromItem and Join
	 * clauses into one single list of Strings
	 * 
	 * @param fromItem It is the name of the first table in the input SQL file
	 * 
	 * @param joins It consists of a list of tables to be joined to the first table
	 * 
	 * @return tables The list of strings containing the names of all the tables
	 * mentioned in the fromItem and joins
	 */
	private static List<String> getTableForSum(FromItem fromItem, List<Join> joins) {
		List<String> tables = new ArrayList<>();
		tables.add(fromItem.toString());

		if (!(joins == null)) {
			for (Join join : joins) {
				tables.add(join.getRightItem().toString());
			}
		}
		return tables;
	}
}
