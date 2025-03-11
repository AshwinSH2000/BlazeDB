package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class JoinOperator extends Operator{
	
	private Operator leftChild;
	private Operator rightChild;
	private Expression joinExpression;
	private Tuple leftTuple;
	private Tuple rightTuple;
	private Map<String, Integer> attributeHashIndex;

	
	
	public JoinOperator(Operator lChild, Operator rChild){//, Expression exp) {
		this.leftChild = lChild;
		this.rightChild = rChild;
		//this.joinExpression = exp;
		this.leftTuple = leftChild.getNextTuple();
		this.rightTuple = null;
		this.attributeHashIndex = null;
		
	
		
	}

	@Override
	public Tuple getNextTuple() {
		
		//begin going through the left table in outer loop. 
		//in the inside loop, iterate over the right table/child
		
		//not sure about while loop.. but lets see
		
		while(leftTuple!=null) {
			if(rightTuple==null) {
				rightTuple = rightChild.getNextTuple();
			}
			
			while(rightTuple!=null) {
				Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
				rightTuple = rightChild.getNextTuple();
				return newlyJoinedTuple;
			}
			
			rightChild.reset();
			leftTuple = leftChild.getNextTuple();
			
		}
		return null;
	}
	
	
	public Tuple concatenateTuples(Tuple lTup, Tuple rTup) {
		
		Tuple joinedTuple = new Tuple();
		joinedTuple.addTuple(lTup);
		joinedTuple.addTuple(rTup);
		
		return joinedTuple;
		
		//created a list of integer before hand but figured out creating a tuple was easier! 
		//also loops present to display the values. 
		
//		List<Integer> joinedTuple = new ArrayList<>(lTup.getTupleValues());
//		System.out.print("Left Tuple = ");
//		for (Integer x: joinedTuple) {
//			System.out.print(x + " , ");
//		}	
//		joinedTuple.addAll(rTup.getTupleValues());
//		System.out.println(" ");
//		System.out.print("Right Tuple = ");
//		for (Integer y: joinedTuple) {
//			System.out.print(y + " , ");
//		}
			
		
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		// TODO Auto-generated method stub
		return attributeHashIndex;
	}

}
