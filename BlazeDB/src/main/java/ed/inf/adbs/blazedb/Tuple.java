package ed.inf.adbs.blazedb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The Tuple class represents a row of data.
 *
 * You will need to modify this class, obviously :).
 */
public class Tuple {
	
	List<Integer> x = new ArrayList<>();
	
	
	
	public void add(int value) {
		x.add(value);
		//System.out.println(value + " is added to the tuple");
	}
	
	//a method to add the entire passed tuple into a current tuple 
	public void addTuple(Tuple tup) {
		for (Integer number : tup.x) {
			x.add(number);
		}
	}

	@Override
	public String toString() {
		String rowData="";
		for(int i : x) {
			if(rowData=="") {
				rowData=rowData+i;
			}
			else
			{
				rowData=rowData+","+i;
			}
		}
		
		return rowData;
	}

	public Integer get(Integer num) {
		return x.get(num);
	}	
	
	public List<Integer> getTupleValues() {
		return x;
	}
	
	public int hashCode() {
		return Objects.hash(x);
	}
	
	public boolean equals(Object passedObject) {
		if (this == passedObject) 
			return true;
		if(passedObject == null || this.getClass() != passedObject.getClass() )
			return false;
		Tuple tuple = (Tuple) passedObject;
		return Objects.equals(x, tuple.getTupleValues());
	}
		
}