package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortOperator extends Operator{

	private List<Tuple> sortBuffer;
	private int index;
	private Map<String, Integer> attributeHashIndex;
	
	public SortOperator(Operator root, List<OrderByElement> orderByElements, Map<String, Integer> attributeHashIndex) {
		this.sortBuffer = new ArrayList<>();
		this.index=0;
		this.attributeHashIndex = attributeHashIndex;
		
		Tuple tuple;
		while(     (tuple=root.getNextTuple())!=null    ) {
			sortBuffer.add(tuple);
		}
		
		
		//what exactly is the code to sort..find out bro
		sortBuffer.sort(new TupleComparator(orderByElements, attributeHashIndex));
	}
	
	
	
	@Override
	public Tuple getNextTuple() {
		
		if (index < sortBuffer.size()) {
            return sortBuffer.get(index++);
        }
        return null; //no more tuples to return..hence returning null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getTableName() {
		return null;
	}

}
