package ed.inf.adbs.blazedb.operator;

import java.io.IOException;

import ed.inf.adbs.blazedb.Tuple;

public class SelectionOperator extends Operator{
	
	Operator root;
	String whereCondition;
	
	public SelectionOperator(Operator root, String whereClause) {
		this.root = root;
		this.whereCondition = whereClause;
	}
	
	/*
	 * @return
	 */
	public boolean checkWhereClause(Tuple tuple) {
		
		
		return false;
	}

	@Override
	public Tuple getNextTuple() {
		//in this function, it must call the getNextTuple of scan operator. 
		//then it needs to check if the tuple matches the where condition.
		//if yes, then maybe return it. 
		
		while(true) {
			Tuple tuple = root.getNextTuple();
			if(tuple==null) {
				return null;
			}
			if(checkWhereClause(tuple)) {
				return tuple;
			}
		}
	}

	@Override
	public void reset() {
		//this is the same code as scanoperator's reset function. change if necessary. 
//		try {
//			reader.close();
//			openFile(); // Reopen the file to reset the iterator
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		//is it required to call the close function explicitly or can i just call root.reset(). need to check
		//will there be any nonsense happening if i call this inside selectionoperator?
		root.reset();
		
	}

}
