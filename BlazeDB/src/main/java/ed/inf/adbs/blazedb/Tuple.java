package ed.inf.adbs.blazedb;

import java.util.ArrayList;
import java.util.List;

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
		
}