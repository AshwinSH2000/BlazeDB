package ed.inf.adbs.blazedb.operator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;

/*
 * This class handles the job of comparing two tuples (list of integers)
 * This is used by the Sort Operator as a custom comparator function while invoking List.sort() method. 
 */
public class TupleComparator implements Comparator<Tuple> {
	
    private List<OrderByElement> orderByElements;
    private Map<String, Integer> attributeHashIndex;
    
    /*
     * This is the Constructor method. 
     * @param orderByElements Contains a list of column attributes to sort the tuples as per their values.
     * @param attributeHashIndex It is a HashMap that maps table attributes to integers representing the attribute's position in the table
     */
    public TupleComparator(List<OrderByElement> orderByElements, Map<String, Integer> attributeHashIndex) {
        this.orderByElements = orderByElements;
        this.attributeHashIndex = attributeHashIndex;
    }
    
    /*
     * Method used to compare the two passed tuples. 
     * It returns 0 if both tuples are equal, 1 if 1st tuple is greater, and -1 if 1st tuple is smaller.
     * @param tuple1 The first tuple to be compared
     * @param tuple2 The second tuple to be compared
     */
	@Override
	public int compare(Tuple tuple1, Tuple tuple2) {
		
		for (OrderByElement orderBy : orderByElements) {
            String columnName = orderBy.getExpression().toString().toLowerCase();

            int index = attributeHashIndex.get(columnName.toLowerCase());
            int tupleOneVal = tuple1.get(index);
            int tupleTwoVal = tuple2.get(index);

            int comparisonResult = Integer.compare(tupleOneVal, tupleTwoVal);
            if (comparisonResult != 0) { 
                return comparisonResult;
            }      
        }
		return 0;
	}

}
