package ed.inf.adbs.blazedb.operator;

import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class JoinOperator extends Operator{
	
	Operator leftChild;
	Operator rightChild;
	Expression joinExpression;
	
	public JoinOperator(Operator lChild, Operator rChild, Expression exp) {
		this.leftChild = lChild;
		this.rightChild = rChild;
		this.joinExpression = exp;
	}

	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
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

}
