package ed.inf.adbs.blazedb;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class represents a tuple (which is list of integers) and all the methods
 * associated with it A tuple class represents a row of data in the database
 *
 * Apart from the normal getters and setters, there are two methods - hashCode()
 * and equals(). These are used explicitly for comparing the tuples in some of
 * the operators.
 */
public class Tuple {

	List<Integer> x = new ArrayList<>();

	/*
	 * A method to add an integer to a tuple
	 * 
	 * @param value The value that needs to be added into the tuple
	 */
	public void add(int value) {
		x.add(value);
	}

	/*
	 * A method to add the integers present in the passed tuple to the referenced
	 * tuples. (Tuple concatenate operator)
	 * 
	 * @param passedTuple This is the tuple passed whose values need to be put into
	 * the referenced tuple.
	 */
	public void addTuple(Tuple passedTuple) {
		for (Integer number : passedTuple.x) {
			x.add(number);
		}
	}

	/*
	 * This method updates one of the integers present inside the tuple(present at
	 * index 'pos') with a new value(num).
	 * 
	 * @param pos The position of the number to be replaced
	 * 
	 * @param num The number which needs to replace the old number at 'pos'
	 */
	public void updateTuple(int pos, int num) {
		x.set(pos, num);
	}

	/*
	 * A custom toString method to print the tuple values separated by comma. Used
	 * just for debugging.
	 */
	@Override
	public String toString() {
		String rowData = "";
		for (int i : x) {
			if (rowData == "") {
				rowData = rowData + i;
			} else {
				rowData = rowData + "," + i;
			}
		}

		return rowData;
	}

	/*
	 * Method to return the number present at the index 'pos' in the referenced
	 * tuple.
	 * 
	 * @param pos The index of the element to return
	 */
	public Integer get(Integer pos) {
		return x.get(pos);
	}

	/*
	 * Method to return the entire tuple
	 */
	public List<Integer> getTupleValues() {
		return x;
	}

	/*
	 * Method to produce the hash code of the tuple calling it. This is used for
	 * comparing the two tuples during sorting but not called explicitly.
	 */
	public int hashCode() {
		return Objects.hash(x);
	}

	/*
	 * This is a custom method to check for equality between two objects(in this
	 * case, Tuples) This method is not explicitly called. It is used by internally.
	 * 
	 * @param passedObject This is an object passed to compare it with the object
	 * that internally called this method.
	 */
	public boolean equals(Object passedObject) {
		if (this == passedObject)
			return true;
		if (passedObject == null || this.getClass() != passedObject.getClass())
			return false;
		Tuple tuple = (Tuple) passedObject;
		return Objects.equals(x, tuple.getTupleValues());

	}

}