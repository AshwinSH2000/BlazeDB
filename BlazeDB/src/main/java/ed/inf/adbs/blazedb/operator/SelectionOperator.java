package ed.inf.adbs.blazedb.operator;

import java.io.IOException;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;

public class SelectionOperator extends Operator{
	
	private Operator root;
	private Expression whereCondition;
	private Map<String, Integer> attributeHashIndex;
	
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
			EvaluateSelection evalSelection = new EvaluateSelection(attributeHashIndex,tuple);

			
			
			if(evalSelection.evaluate(whereCondition)) {
				return tuple;
			}
		}
	}

	@Override
	public void reset() {

		
		//is it required to call the close function explicitly or can i just call root.reset(). need to check
		//will there be any nonsense happening if i call this inside selectionoperator?
		root.reset();
		
	}



	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		// TODO Auto-generated method stub
		return attributeHashIndex;
	}



	@Override
	protected String getTableName() {
		// TODO Auto-generated method stub
		return root.getTableName();
	}

}
