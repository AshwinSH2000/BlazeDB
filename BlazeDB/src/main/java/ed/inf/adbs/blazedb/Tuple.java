package ed.inf.adbs.blazedb;

import java.util.ArrayList;
import java.util.List;

/**
 * The Tuple class represents a row of data.
 *
 * You will need to modify this class, obviously :).
 */
public class Tuple {
	
	List<Integer> x = new ArrayList<>();;
	
	public void add(int int1) {
		// TODO Auto-generated method stub
		System.out.println(int1 + " is inside the add function");
		x.add(int1);
	}
	
	
	//need to implement add function that is called in scanoperator
	
	
}