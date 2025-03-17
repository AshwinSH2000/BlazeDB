package ed.inf.adbs.blazedb.operator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class TupleComparator implements Comparator<Tuple> {

	
    private List<OrderByElement> orderByElements;
    private Map<String, Integer> attributeHashIndex;
    
    public TupleComparator(List<OrderByElement> orderByElements, Map<String, Integer> attributeHashIndex) {
        this.orderByElements = orderByElements;
        this.attributeHashIndex = attributeHashIndex;
    }
	@Override
	public int compare(Tuple tuple1, Tuple tuple2) {
		
		for (OrderByElement orderBy : orderByElements) {
            String columnName = orderBy.getExpression().toString().toLowerCase();

            //int index = tuple1.getColumnIndex(columnName); 
            //need to get atttributehashIndex
            
        	System.out.println("raise ypur armsm up and singgg..om shanthi oommm "+attributeHashIndex.keySet().toString());
        	System.out.println("raise ypur armsm up and singgg..om shanthi oommm "+attributeHashIndex.values().toString());
        	System.out.println(orderBy.getExpression().toString().toLowerCase()+" is "+attributeHashIndex.get(orderBy.getExpression().toString().toLowerCase()));
        	System.out.println("raise ypur armsm up and singgg..om shanthi oommm "+columnName.toLowerCase());

            int index = attributeHashIndex.get(columnName.toLowerCase());
            int value1 = tuple1.get(index);
            int value2 = tuple2.get(index);

            int comparisonResult = Integer.compare(value1, value2);
            if (comparisonResult != 0) { // Only return if a difference is found
                return comparisonResult;
            }
            
            
        }
		
		return 0;
	}

}
