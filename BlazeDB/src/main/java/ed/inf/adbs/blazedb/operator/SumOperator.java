package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;

/*
 * This class handles the GROUPBY() and SUM() operators in the given SQL code. 
 * It primarily has four divisions - 1. An optimisation case where Select has only SUM(integer) or SUM(product of ints)
 * 									  2. Clauses containing only SUM() functions
 * 									  3. Clauses containing only GROUP BY clauses
 * 									  4. Clauses containing both SUM() and GROUP BY clauses
 * 
 * In the 1st case, since the actual data does not matter, I iterate over each table to count the number of rows using ScanOperator.
 * Then multiply all these to get the number of rows in the final table (cross join case)
 * In the 2nd case, I iterate over each entry in the SELECT clause and perform the sum in case there is just 1 column/number inside SUM().
 * If there are more than 1 numbers/columns inside SUM, I iterate over them using a loop to first multiply them and later sum everything. 
 * 
 * In the 3rd case, I iterate over all the Attributes mentioned in GROUP BY clause. Then pick up all the attributes mentioned in the Select clause
 * (as select can contain the subset of GROUP BY)into a tuple and later add this tuple into a HashMap
 * (HashMap's key = all values of GROUPBY attributes of tuple, HashMap's value = The values of columns to be projected after GROUPBY)
 * This helps to avoid duplicates and gives only those attributes that were asked to be projected after group by.
 * 
 * In the 4th case, again I divide them into two 1. If the tuple is present in the HashMap(as a key) of group by elements
 * 												 2. If the tuple is not present in the HashMap(as a key) of group by elements. 
 * If it is not present, either add it directly or perform the product of terms and add it to the HashMap (as a value)
 * If it is present, then extract the tuple based on the GROUP BY key, update the sum as sum + new tuple's values and put it back to HashMap (as a value)
 * Using a HashMap allows to hold only unique values and at the same time allows to extract and update values
 */
public class SumOperator extends Operator {

	private Operator root;
	private ExpressionList groupByClause;
	private List<SelectItem<?>> selectClause;
	private Map<String, Integer> attributeHashIndex;
	private Map<String, Integer> projectedAttributeHashIndex;
	private int index;
	private List<Tuple> bufferTuples;
	private List<Tuple> outputTuples;
	private List<String> combinedTableNames;
	private Expression whereClause;

	/*
	 * Constructor for SumOperator
	 * 
	 * @param root The child operator passed from the calling method
	 * 
	 * @param groupByClause The ExpressionList containing all the group by clauses
	 * mentioned in the input SQL file
	 * 
	 * @param selectClause The clauses mentioned after SELECT in the input SQL file.
	 * Used to see what columns need to be projected.
	 * 
	 * @param attributesHashIndex HashMap that maps table attributes to integers
	 * representing the attribute's position in the table
	 */
	public SumOperator(Operator root, ExpressionList groupByClause, List<SelectItem<?>> selectClause,
			Map<String, Integer> attributesHashIndex, List<String> combinedTableNames, Expression WHERE) {
		this.root = root;
		this.groupByClause = groupByClause;
		this.selectClause = selectClause;
		this.attributeHashIndex = attributesHashIndex;
		this.index = 0;
		this.bufferTuples = new ArrayList<>();
		this.outputTuples = new ArrayList<>();
		this.combinedTableNames = combinedTableNames;
		this.projectedAttributeHashIndex = new HashMap<>();
		this.whereClause = WHERE;

		groupTheTuples();
	}

	/*
	 * This method iterates through the tuples given by the child operator and
	 * decides which case of execution to proceed with. It checks the select clause
	 * and group by clause and decides one of the three ways to proceed. 1. Clauses
	 * containing only SUM(integer), SUM(product of integers) In case the Select
	 * clause consists of only sum() functions that have just constants in it, then
	 * the data of the tables are not necessary to compute it. Hence an optimisation
	 * is applied here. It just scans the tables, counts the number of tuples and
	 * displays it. 2. Clauses containing only SUM() functions 3. Clauses containing
	 * only GROUP BY clauses 4. Clauses containing both SUM() and GROUP BY clauses
	 * All the three methods put the final grouped by tuples into a List of tuples
	 * called outputTuples
	 */
	private void groupTheTuples() {
		
		Tuple tuple;
		while ((tuple = root.getNextTuple()) != null) {
			bufferTuples.add(tuple);
		}

		// if there are no tuples obtained from the child operator, return immediately
		// and avoid all unnecessary computations below
		if (bufferTuples.size() == 0) {
			return;
		}
		Tuple result = new Tuple();

		if (selectClause.toString().toLowerCase().contains("sum") && checkForTables(selectClause, combinedTableNames)
				&& groupByClause == null && whereClause==null) {
			int product = 1;
			for (String table : combinedTableNames) {
				ScanOperator scan = new ScanOperator(table);
				int count = 0;
				while (scan.getNextTuple() != null)
					count++;
				product *= count;
				scan.close();
			}
			int tupleValue = 1;
			for (SelectItem item : selectClause) {
				tupleValue = evaluateTuple(item);
				result.add(tupleValue * product);
			}
			outputTuples.add(result);
		} else {
			if (selectClause.toString().toLowerCase().contains("sum") && groupByClause == null) {
				// This is a case where SUM() function is present but there is no GroupBy clause
				onlySumClause();
			}

			if (!selectClause.toString().toLowerCase().contains("sum") && groupByClause != null) {
				// This is a case where SUM() function is not at all present by there is GroupBy
				// clause
				onlyGroupByClause();
			}

			if (selectClause.toString().toLowerCase().contains("sum") && groupByClause != null) {
				// This is the last case where both SUM() and GroupBy are present together
				sumAndGroupBy();
			}
		}
	}

	/*
	 * A method to execute the SQL statements containing only SUM functions. It can
	 * either be of the form SUM(integer/column) or SUM(integer/column *
	 * integer/column [ * integer/column ...]) or mix of these two with any number
	 * of sum functions.
	 * 
	 * Iterates over Select clause and performs the sum of necessary columns
	 */
	private void onlySumClause() {

		Tuple tupleToReturn = new Tuple();
		// need to set just a tuple
		// since group by clause is null, i can assume that there is no other selectitem
		// apart from sum(s)
		int sum = 0, evalSumExpr = 1, count = 0;

		for (SelectItem<?> sumItem : selectClause) {
			evalSumExpr = 1;
			sum = 0;
			if (sumItem instanceof SelectItem) {
				Expression exp = ((SelectItem) sumItem).getExpression();
				if (exp instanceof Function && exp.toString().toLowerCase().contains("sum")) {
					Function function = (Function) exp;

					Expression parameters = function.getParameters();

					if (!parameters.toString().contains("*")) {
						// Does not contain * which means its either sum(number) or sum(column)

						// If it contains column
						if (attributeHashIndex.containsKey(parameters.toString().toLowerCase())) {
							for (Tuple tempTuple : bufferTuples) {
								sum += tempTuple.get(attributeHashIndex.get(parameters.toString().toLowerCase()));

							}
						}

						// If it contains a single number inside brackets
						else {
							evalSumExpr = Integer.parseInt(parameters.toString());
							sum = bufferTuples.size() * evalSumExpr;

						}
					} else {
						// In this case we check for either sum(number * number [* number ...]) or
						// sum(column * column [* column...]) or mix of two
						int ans = 1, number = 0;
						sum = 0;
						evalSumExpr = 1;
						String[] numbers = parameters.toString().split("\\*");

						for (Tuple currentTuple : bufferTuples) {
							ans = 1;
							for (String individualNums : numbers) {
								individualNums = individualNums.strip();

								// If it is a column reference
								if (attributeHashIndex.containsKey(individualNums.toLowerCase())) {
									number = currentTuple.get(attributeHashIndex.get(individualNums.toLowerCase()));
								} else {
									// If it is a number inside
									number = Integer.parseInt(individualNums);
								}
								ans = ans * number;
							}
							sum = sum + ans;
						}
					} // calculate the sum based on the number of tuples in bufferTuple
				} else {
					return;
				}

			}
			tupleToReturn.add(sum);
			projectedAttributeHashIndex.put(sumItem.toString().toLowerCase(), count++);
		}
		outputTuples.add(tupleToReturn);

	}

	/*
	 * Method to handle queries containing only the GROUP BY clause. It does not
	 * contain any SUM() function at all Did not do an explicit check to verify if
	 * the condition in select clause matches the condition in group by clause. For
	 * a valid SQL statement, Select must contain a subset of group by. Using this
	 * to proceed.
	 */
	private void onlyGroupByClause() {

		HashMap<Tuple, Tuple> groupByTuples = new HashMap<Tuple, Tuple>();

		for (Tuple scannedTuple : bufferTuples) {
			Tuple tupleHashValue = new Tuple();
			Tuple tupleHashKey = new Tuple();
			for (Object groupByObj : groupByClause) {

				// This if clause helps to filter out attributes which are present in GroupBy
				// but not in Select
				if (selectClause.toString().toLowerCase().contains(groupByObj.toString().toLowerCase())) {
					tupleHashValue.add(scannedTuple.get(attributeHashIndex.get(groupByObj.toString().toLowerCase())));
				}
				tupleHashKey.add(scannedTuple.get(attributeHashIndex.get(groupByObj.toString().toLowerCase())));
			}
			groupByTuples.put(tupleHashKey, tupleHashValue);

		}

		int count = 0;
		// Put this in a new loop because it needs to execute the loop only once.
		// In the earlier instance the inner loop is a part of a bigger FOR loop
		for (Object groupByObj : groupByClause) {
			if (selectClause.toString().toLowerCase().contains(groupByObj.toString().toLowerCase())) {

				projectedAttributeHashIndex.put(groupByObj.toString().toLowerCase(), count++);
			}
		}
		for (Map.Entry<Tuple, Tuple> entry : groupByTuples.entrySet()) {
			outputTuples.add(entry.getValue());

		}

	}

	/*
	 * Method to handle queries having both SUM() and GROUP BY clauses. This feels
	 * like a large monolithic block of code but I kept it as is because it
	 * encapsulates distinct logic of handling both SUM & GROUPBY For this
	 * submission, I prioritized stability of this code block over further
	 * refactoring.
	 */
	private void sumAndGroupBy() {

		HashMap<Tuple, Tuple> uniqueTuples = new HashMap<Tuple, Tuple>();

		// Mix of both. So, first need to group by and then apply the sum.
		for (Tuple scannedTuple : bufferTuples) {
			Tuple tupleHashKey = new Tuple();

			for (Object obj : groupByClause) {
				tupleHashKey.add(scannedTuple.get(attributeHashIndex.get(obj.toString().toLowerCase())));
			}
			Tuple tupleHashValue = new Tuple();
			int colValue = 0;

			if (uniqueTuples.containsKey(tupleHashKey)) {
				Tuple presentTuple = uniqueTuples.get(tupleHashKey);
				int presentValue = 0;
				for (SelectItem<?> item : selectClause) {
					Expression exp = ((SelectItem) item).getExpression();
					if (exp instanceof Function) {
						Function function = (Function) exp;

						Expression parameters = function.getParameters();
						String[] stringParameters = parameters.toString().split("\\*");
						if (stringParameters.length == 1) {

							try {
								colValue = Integer.parseInt(stringParameters[0].strip());
							} catch (Exception e) {
								colValue = scannedTuple.get(attributeHashIndex.get(stringParameters[0].toLowerCase()));
							}

							presentValue = presentTuple
									.get(projectedAttributeHashIndex.get(exp.toString().toLowerCase()));
							tupleHashValue.add(presentValue + colValue);
						} else {
							// It means parameters are more than 1
							int product = 1;
							for (String indvParams : stringParameters) {

								try {
									colValue = Integer.parseInt(indvParams.strip());
									product = product * colValue;
								} catch (Exception e) {
									colValue = scannedTuple
											.get(attributeHashIndex.get(indvParams.strip().toLowerCase()));
									product = product * colValue;
								}
							}

							presentValue = presentTuple
									.get(projectedAttributeHashIndex.get(exp.toString().toLowerCase()));
							tupleHashValue.add(product + presentValue);
						}
					} else {
						// it is not a function at all.. which means a normal column that must be summed
						// here.
						colValue = scannedTuple.get(attributeHashIndex.get(exp.toString().toLowerCase()));
						tupleHashValue.add(colValue);
					}

				}
				uniqueTuples.put(tupleHashKey, tupleHashValue);

			} else {
				int counter = 0;
				// Iterate over the select clause and select the columns to be put into it
				for (SelectItem<?> item : selectClause) {

					Expression exp = ((SelectItem) item).getExpression();

					if (exp instanceof Function) {
						// eg sum(integer) or sum(column)
						Function function = (Function) exp;

						Expression parameters = function.getParameters();

						String[] stringParameters = parameters.toString().split("\\*");

						if (stringParameters.length == 1) {

							try {
								colValue = Integer.parseInt(stringParameters[0].strip());
							} catch (Exception e) {
								colValue = scannedTuple.get(attributeHashIndex.get(stringParameters[0].toLowerCase()));
							}
							tupleHashValue.add(colValue);
							projectedAttributeHashIndex.putIfAbsent(exp.toString().toLowerCase(), counter++);

						} else {
							// It means the parameters are more than one
							// we do not know if it is a column value of integer.. so iterate over all and
							// check in each chase
							int product = 1;
							for (String indvParams : stringParameters) {

								try {
									colValue = Integer.parseInt(indvParams.strip());
									product = product * colValue;
								} catch (Exception e) {
									colValue = scannedTuple
											.get(attributeHashIndex.get(indvParams.strip().toLowerCase()));
									product = product * colValue;
								}
							}
							tupleHashValue.add(product);
							projectedAttributeHashIndex.putIfAbsent(exp.toString().toLowerCase(), counter++);
						}
					} else {
						// since it is not a function it has to be a normal column of the table
						colValue = scannedTuple.get(attributeHashIndex.get(exp.toString().toLowerCase()));
						tupleHashValue.add(colValue);

						projectedAttributeHashIndex.putIfAbsent(exp.toString().toLowerCase(), counter++);
						// This else block executes in case a constant is passed after SELECT eg. SELECT
						// 5 FROM Student.
						// But since its stated SELECT clause will either specify a subset of columns or
						// have the form SELECT *, I have not included a check for plain integers.

					}
				}
				uniqueTuples.put(tupleHashKey, tupleHashValue);
			}
		}

		for (Map.Entry<Tuple, Tuple> entry : uniqueTuples.entrySet()) {
			outputTuples.add(entry.getValue());
		}
	}

	/*
	 * Method that returns the tuples one by one after GROUPING and/or SUMMING over
	 * specified attributes. Since SumOperator is a blocking operator, it does all
	 * the calculations beforehand and only returns the tuples one by one in this
	 * method
	 * 
	 * @return (tuple) The tuple in the sequential order of the table.
	 */
	@Override
	public Tuple getNextTuple() {

		if (index < outputTuples.size()) {
			return outputTuples.get(index++);
		}
		return null;
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

	/*
	 * Method to check if there is any instance of a table mentioned in the Select
	 * clause. It is used to filter queries that contain only SUM( integer(s) )
	 * functions. In case there is a tableName.columnName inside the SUM() function,
	 * it returns false. Else true.
	 * 
	 * @param SELECT It is a list of selectItems is expected to be printed in the
	 * output file
	 * 
	 * @param tableNames It is a list of all the tables present in the input SQL
	 * file
	 * 
	 * @return (boolean) The result after checking if the mentioned tables'
	 * attributes are present in the select clause or not
	 */
	private static boolean checkForTables(List<SelectItem<?>> SELECT, List<String> tableNames) {

		if (tableNames == null) {
			return false;
		}
		for (String table : tableNames) {
			if (SELECT.toString().toLowerCase().contains(table.toLowerCase())) {
				return false;
			}
		}
//		if (SELECT.toString().toLowerCase().contains(FROM.toString().toLowerCase()))
//			return false;
//		if (!(JOIN == null)) {
//			for (Join join : JOIN) {
//				if (SELECT.toString().toLowerCase().contains(join.toString().toLowerCase()))
//					return false;
//			}
//		}

		return true;
	}

	/*
	 * Method to take a selectItem and evaluate its answer. If it is a just a number
	 * inside SUM(), it returns the same. If it more than 1 numbers inside SUM(), it
	 * performs their product and returns it.
	 * 
	 * @param item The SelectItem to be evaluated. Here it is always a SUM()
	 * function.
	 * 
	 * @return answer The result after evaluating the number inside SUM() function
	 */
	private int evaluateTuple(SelectItem item) {
		int answer = 0;
		Expression exp = ((SelectItem) item).getExpression();
		Function function = (Function) exp;
		Expression parameters = function.getParameters();
		if (!parameters.toString().contains("*")) {
			// Does not contain * which means its either sum(number)
			answer = Integer.parseInt(parameters.toString());
		} else {
			// In this case we check for either sum(number * number [* number ...])
			answer = 1;
			String[] numbers = parameters.toString().split("\\*");
			for (String individualNums : numbers) {
				individualNums = individualNums.strip();
				answer = answer * Integer.parseInt(individualNums);
			}
		}
		return answer;
	}

	/*
	 * Method used to reset the state of the operator and ask it to return tuples
	 * from the very beginning.
	 */
	@Override
	public void reset() {
		root.reset();
	}

	/*
	 * getter function to return the hash index (schema of the table after
	 * projection).
	 * 
	 * @return projectedAttributeHashIndex HashMap mapping table attributes to their
	 * corresponding integer position values after projection
	 */
	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		return this.projectedAttributeHashIndex;
	}

	/*
	 * Getter method to return the Name of the table
	 * 
	 * @return (String) returns the name of the table that is used passed to the
	 * child operator
	 */
	@Override
	protected String getTableName() {
		return root.getTableName();
	}

}
