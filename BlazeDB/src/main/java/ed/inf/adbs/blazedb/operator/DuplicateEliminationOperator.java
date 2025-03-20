package ed.inf.adbs.blazedb.operator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ed.inf.adbs.blazedb.Tuple;

public class DuplicateEliminationOperator extends Operator{
	
	private Operator root;
	private HashSet<Tuple> uniqueTuples;
	//private Tuple currentTuple;
	
	public DuplicateEliminationOperator(Operator root) {
		this.root = root;
		uniqueTuples = new HashSet<Tuple>();
	}
	@Override
	public Tuple getNextTuple() {
		
		Tuple currentTuple = root.getNextTuple();
		while(   currentTuple!=null ) {
			
			if(  !uniqueTuples.contains(currentTuple)   ) {
				uniqueTuples.add(currentTuple);
				System.out.println("DUPELIMOP: "+currentTuple.toString()+" is a unique tuple");
				System.out.println("DUPELIMOP: "+currentTuple.hashCode()+"\n");
				for(Tuple individualTuple:uniqueTuples)
					System.out.println(individualTuple.hashCode());
				return currentTuple;
			}
			else {
				System.out.println(("DUPELIMOP: "+currentTuple.toString()+" is not a unique tuple"));
				
			}
			currentTuple = root.getNextTuple();
		}
		return null;
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
		// TODO Auto-generated method stub
		return null;
	}

}
