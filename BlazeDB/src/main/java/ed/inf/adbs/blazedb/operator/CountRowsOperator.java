package ed.inf.adbs.blazedb.operator;

import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.SelectItem;

/*
 * This Class is specifically written to optimise the queries which ask for SUM(integer) or SUM(integer * integer [* integer...]) 
 * Here it initially assigns a scan operator to each table and iterates over them finding the number of tuples in each of them. 
 * If there is just 1 table present, then that number is the number of rows in it. 
 * If there are more than 1 table, then it finds the product of all the numbers which is the number of rows present in the cross join of all those tables. 
 * This was for queries having SUM(1) in them. 
 * 
 * I have extended this to work for queries having any integer or product of integers inside SUM. 
 */
public class CountRowsOperator extends Operator {

	private int product;
	private boolean hasReturned;
	private List<SelectItem<?>> SELECT;
	private Tuple sumTuple;

	/*
	 * Constructor method. It first iterates over all the tables and finds the
	 * number of tuples in their cross join. Then depending upon the number of SUM()
	 * functions present and the parameters inside it, it prepares a tuple and
	 * stores it in sumTuple
	 * 
	 * @param SELECT It is a List of all the attributes needed to be projected in
	 * the result.
	 * 
	 * @param tableNames It is a list of all the tableNames mentioned in the SQL
	 * input file.
	 */
	public CountRowsOperator(List<SelectItem<?>> SELECT, List<String> tableNames) {
		this.product = 1;
		Tuple temp = new Tuple();
		this.hasReturned = false;
		this.SELECT = SELECT;
		for (String tableName : tableNames) {
			ScanOperator scan = new ScanOperator(tableName);
			int count = 0;
			while (scan.getNextTuple() != null) {
				count++;
			}
			product *= count;
			scan.close();
		}
		int tupleValue = 1;
		for (SelectItem item : SELECT) {
			tupleValue = evaluateTuple(item);
			temp.add(tupleValue * product);
		}
		sumTuple = temp;
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
	 * Method to display the result of this Operator. 
	 * The result of the SUM() functions have already been calculated and stored in sumTuple. 
	 * This method sends the tuple when called. 
	 * @return sumTuple It is a tuple that contains the result of all the SUM() functions
	 */
	@Override
	public Tuple getNextTuple() {
		if (!hasReturned) {
			hasReturned = true;
			return sumTuple;
		}
		return null;
	}

	/*
	 * Method implemented due to inheriting Operator class. Not of much use here.
	 */
	@Override
	public void reset() {
		hasReturned = false;
	}

	/*
	 * Method implemented due to inheriting Operator class. Not of much use here.
	 */
	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		return null;
	}

	/*
	 * Method implemented due to inheriting Operator class. Not of much use here.
	 */
	@Override
	protected String getTableName() {
		return null;
	}

}
