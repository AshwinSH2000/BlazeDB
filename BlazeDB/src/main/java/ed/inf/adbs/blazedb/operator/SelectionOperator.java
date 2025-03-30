package ed.inf.adbs.blazedb.operator;

import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

/*
 * This class defines the SelectionOperator which takes a WHERE condition and returns only those tuples
 * which satisfy the condition.
 */
public class SelectionOperator extends Operator{
	
	private Operator root;
	private Expression whereCondition;
	private Map<String, Integer> attributeHashIndex;
	
	/*
	 * Constructor for this class. 
	 * @param root The child operator of this Selection Operator
	 * @param whereCondition This is an Expression that has one or many WHERE clauses joined by AND clause
	 * @param attributeHashIndex HashMap that maps table attributes to integers representing the attribute's position in the table
	 */
	public SelectionOperator(Operator root, Expression whereCondition, Map<String, Integer> attributeHashIndex) {
		this.root = root;
		this.whereCondition = whereCondition;
		this.attributeHashIndex = attributeHashIndex;
	}
	
	/*
	 * This method scans each tuple passed by its child operator and evaluates it against all the WHERE clause passed. 
	 * If it satisfies all the conditions/clause then it is returned back to be used for further operations or to be printed to file. 
	 * @return tuple The tuple which satisfies the conditions specified in the SQL query
	 */
	@Override
	public Tuple getNextTuple() {
		
		while(true) {
			Tuple tuple = root.getNextTuple();
			if(tuple==null) {
				return null;
			}
			EvaluateSelection evalSelection = new EvaluateSelection(attributeHashIndex,tuple);

			if(evalSelection.evaluate(whereCondition)) {
				return tuple;
			}
		}
	}

	
	/*
	 * Method used to reset the state of the operator and ask it to return tuples from the very beginning. 
	 * Closing and reopening the file sets it to read it from the beginning
	 */
	@Override
	public void reset() {
		root.reset();	
	}


	/*
	 * getter function to return the hash index. 
	 * @return attributeHashIndex HashMap mapping table attributes to their corresponding integer position values
	 */
	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		return attributeHashIndex;
	}



	/*
	 * Getter method to return the Name of the table
	 * @return (String) returns the name of the table that is used passed to the child operator
	 */	
	@Override
	protected String getTableName() {
		return root.getTableName();
	}

}
