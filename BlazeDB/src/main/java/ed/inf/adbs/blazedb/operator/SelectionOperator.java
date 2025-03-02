package ed.inf.adbs.blazedb.operator;

import java.io.IOException;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;

public class SelectionOperator extends Operator{
	
	Operator root;
	Expression whereCondition;
	Map<String, Integer> attributeHashIndex;
	
	public SelectionOperator(Operator root, Expression whereCondition, Map<String, Integer> attributeHashIndex) {
		this.root = root;
		this.whereCondition = whereCondition;
		this.attributeHashIndex = attributeHashIndex;
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
			System.out.println("BEFOOOOOOOOORRRRRRRRREEEEEEEEEEEEEE");
			EvaluateSelection evalSelection = new EvaluateSelection(attributeHashIndex,tuple);

//			
//			if (whereCondition instanceof BinaryExpression) {
//			    System.out.println("WHERE condition is a BinaryExpression!");
//			} else {
//			    System.out.println("WHERE condition is: " + whereCondition.getClass().getSimpleName());
//			}
//			
			
			
			if(evalSelection.evaluate(whereCondition)) {
				System.out.println(tuple + "is one of the tuple tp satidfy the where clause");
				return tuple;
			}
			System.out.println("AFTTTTTTTTTTTTTTTEEEEEEEEEERRRRRRRR");
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



	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		// TODO Auto-generated method stub
		return null;
	}

}
