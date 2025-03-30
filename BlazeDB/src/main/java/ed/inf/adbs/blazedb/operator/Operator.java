package ed.inf.adbs.blazedb.operator;

import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;

/**
 * The abstract Operator class for the iterator model.
 *
 * Feel free to modify this class, but must keep getNextTuple() and reset()
 */
public abstract class Operator {

	/**
	 * Retrieves the next tuple from the iterator.
	 * 
	 * @return A Tuple object representing the row of data, or NULL if EOF reached.
	 */
	public abstract Tuple getNextTuple();

	/**
	 * Resets the iterator to the start.
	 */
	public abstract void reset();

	/**
	 * To get the details of the mapping between column name and corresponding
	 * integer
	 * 
	 * @return A hashMap with attributes as keys and its integer position in the
	 *         schema
	 */
	protected abstract Map<String, Integer> getAttributeHashIndex();

	/*
	 * To return the name of the table calling this operator
	 */
	protected abstract String getTableName();
}