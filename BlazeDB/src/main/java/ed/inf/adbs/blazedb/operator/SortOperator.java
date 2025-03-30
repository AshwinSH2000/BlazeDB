package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;

/*
 * This class is for sorting the tuples as per the attributes mentioned in the ORDER BY clause.
 * It first reads all the tuples from its child and then calls List.sort() using a custom comparator.
 * Then sends the output tuple one by one to another operator or to be printed in the file. 
 */
public class SortOperator extends Operator {

	private List<Tuple> sortBuffer;
	private int index;
	private Map<String, Integer> attributeHashIndex;

	/*
	 * Constructor for sort operator. Since this is a blocking operator, it first
	 * reads all the information into a list of tuples. Then it invokes List.sort()
	 * function along with a custom comparator that sorts the list of tuples in
	 * ascending order.
	 * 
	 * @param root The root or child operator passed from the invoking method
	 * 
	 * @param orderByElements Contains a list of column attributes to sort the
	 * tuples as per their values.
	 * 
	 * @param attributeHashIndex HashMap that maps table attributes to integers
	 * representing the attribute's position in the table
	 */
	public SortOperator(Operator root, List<OrderByElement> orderByElements, Map<String, Integer> attributeHashIndex) {
		this.sortBuffer = new ArrayList<>();
		this.index = 0;
		this.attributeHashIndex = attributeHashIndex;

		Tuple tuple;
		while ((tuple = root.getNextTuple()) != null) {
			sortBuffer.add(tuple);
		}

		// If no tuples are pulled from the child operator, avoid unnecessary
		// computations and return immediately
		if (sortBuffer.size() == 0) {
			return;
		}
		sortBuffer.sort(new TupleComparator(orderByElements, attributeHashIndex));
	}

	/*
	 * Method to return the contents of sorted List of tuples row(tuple)wise. Since
	 * the List is already sorted above, here it is just being returned.
	 * 
	 * @return tuple It contains the tuple that is to be returned to the calling
	 * function
	 */
	@Override
	public Tuple getNextTuple() {

		if (index < sortBuffer.size()) {
			return sortBuffer.get(index++);
		}
		return null; // no more tuples to return..hence returning null;
	}

	/*
	 * This method is not of much use here. But since it is a part of Operator
	 * class, it has to be created here
	 */
	@Override
	public void reset() {

	}

	/*
	 * getter function to return the attribute hash map containing mapping from
	 * attribute to integer positions in table
	 * 
	 * @return attributeHashIndex HashMap mapping table attributes to their
	 * corresponding integer position values
	 */
	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * Getter method to return the name of the table
	 * 
	 * @return tableName Contains the name of the table to be returned
	 */
	@Override
	protected String getTableName() {
		return null;
	}

}
