package ed.inf.adbs.blazedb.operator;

import java.util.HashSet;
import java.util.Map;
import ed.inf.adbs.blazedb.Tuple;

/*
 * This is a class that performs Duplicate Elimination from the tuples that it receives. 
 * It inserts each tuple into a HashSet if the HashSet does not contain that tuple. 
 * At the end of scanning all the tuples, it will have a HashSet of only unique values. 
 */
public class DuplicateEliminationOperator extends Operator {

	private Operator root;
	private HashSet<Tuple> uniqueTuples;

	/*
	 * This is the constructor for this class.
	 * 
	 * @param root This is the child operator passed while building query plan
	 */
	public DuplicateEliminationOperator(Operator root) {
		this.root = root;
		uniqueTuples = new HashSet<Tuple>();
	}

	/*
	 * This is the the method used to iterate through all the tuples passed by the
	 * child operator. It checks if each tuple is present in the HashSet. If not
	 * present, it adds it to the HashSet. Else continues.
	 * 
	 * @return currentTuple This is the tuple that the operator found to be distinct
	 * i.e. it was not present in the HashSet
	 */
	@Override
	public Tuple getNextTuple() {

		Tuple currentTuple = root.getNextTuple();
		while (currentTuple != null) {

			if (!uniqueTuples.contains(currentTuple)) {
				uniqueTuples.add(currentTuple);

				return currentTuple;
			}

			currentTuple = root.getNextTuple();
		}
		return null;
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
	 * This is a method used to return the root's HashMap that contains a mapping
	 * from table attributes to integers representing the respective attribute's
	 * position in the tuple.
	 * 
	 * @return root.attributeHashIndex() The hashMap returned by the root's
	 * getAttributeHashIndex() method is returned by this method.
	 */
	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		// TODO Auto-generated method stub
		return root.getAttributeHashIndex();
	}

	/*
	 * Method used to return the root's (child) tableName
	 * 
	 * @return The name of the table the root of this operator is referring to.
	 */
	@Override
	protected String getTableName() {
		// TODO Auto-generated method stub
		return root.getTableName();
	}

}
