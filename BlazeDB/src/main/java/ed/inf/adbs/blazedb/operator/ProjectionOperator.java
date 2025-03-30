package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

/*
 * This class deals with applying Projection on the tables. 
 * 
 */
public class ProjectionOperator extends Operator {

	private Operator root;
	private List<SelectItem<?>> SELECT;
	private Map<String, Integer> attributeHashIndex;
	private List<String> colsToBeProjected;
	private Map<String, Integer> projectedAttributesHashIndex;

	/*
	 * This Method is the constructor of this class. It generates two new variables
	 * - colsToBeProjected (list containing all the columns to be projected)
	 * projectedAttributesHashIndex (HashMap of the resulting table - Maps
	 * attributes to integers representing its position in the tuple/table)
	 * 
	 * @param root The operator child of this Projection operator
	 * 
	 * @param SELECT The list of all the things(columns or functions) that needs to
	 * be projected
	 * 
	 * @param attributeHashIndex The HashMap that maps attributes to the integer
	 * representing its position in the tuple/table
	 */
	public ProjectionOperator(Operator root, List<SelectItem<?>> SELECT, Map<String, Integer> attributeHashIndex) {
		this.root = root;
		this.SELECT = SELECT;
		this.attributeHashIndex = attributeHashIndex;
		this.projectedAttributesHashIndex = new HashMap<>();

		colsToBeProjected = new ArrayList<>();

		int index = 0;
		for (SelectItem<?> col : this.SELECT) {

			if (col instanceof SelectItem) {
				Expression exp = ((SelectItem) col).getExpression();

				if (exp instanceof Column) {
					Column column = (Column) exp;
					colsToBeProjected.add(column.toString().toLowerCase());
					projectedAttributesHashIndex.put(column.toString().toLowerCase(), index++);

				}
			}
		}
	}

	/*
	 * Method to look at the passed tuple and extract the values of those columns
	 * which need to be projected and add it to an output tuple
	 * 
	 * @param tuple This is a tuple passed from getNextTuple method from which we
	 * need to extract the necessary values
	 * 
	 * @return projectedTuple The tuple consisting of values projected from the
	 * original passed tuple
	 */
	public Tuple ProjectCol(Tuple tuple) {

		Tuple projectedTuple = new Tuple();

		for (String col : colsToBeProjected) {
			if (attributeHashIndex.containsKey(col)) {
				projectedTuple.add(tuple.get(attributeHashIndex.get(col)));
			}
		}
		return projectedTuple;
	}

	/*
	 * The Method to iterate over all the tuples coming from its child and pass it
	 * to extract the necessary columns.
	 * 
	 * @return (tuple) The tuple consisting only of values projected from the
	 * original tuple
	 */
	@Override
	public Tuple getNextTuple() {

		while (true) {
			Tuple tuple = root.getNextTuple();
			if (tuple == null) {
				return null;
			}
			return ProjectCol(tuple);
		}
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
	 * Method to return the new HashMap containing the mapping of projected columns
	 * to integers representing its position in the new table
	 * 
	 * @return projectedAttributesHashIndex HashMap that maps projected attributes
	 * to integer positions in table (such as student.a=0, student.b=1)
	 */
	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		return projectedAttributesHashIndex;
	}

	/*
	 * Method to return the table name used in the child of Project operator.
	 * 
	 * @return (string) name of the table used in the child operator.
	 */
	@Override
	protected String getTableName() {
		return root.getTableName();
	}

}
