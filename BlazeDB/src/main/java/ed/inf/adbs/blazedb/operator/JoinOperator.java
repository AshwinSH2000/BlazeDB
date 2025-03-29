package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;


/*
 * This is the class of JoinOperators. At any instance, it takes two tables and performs a join on them based on the join conditions.
 * It splits the entire code into two halves. One with a join condition, another without join condition. 
 * 
 */
public class JoinOperator extends Operator{
	
	private Operator leftChild;
	private Operator rightChild;
	private List<Expression> joinExpression;
	private Tuple leftTuple;
	private Tuple rightTuple;
	private Map<String, Integer> leftAttributeHashIndex;
	private Map<String, Integer> rightAttributeHashIndex;


	
	
	public JoinOperator(Operator lChild, Operator rChild){
		this.leftChild = lChild;
		this.rightChild = rChild;
		this.joinExpression = null;
		this.leftTuple = leftChild.getNextTuple();
		this.rightTuple = null;
		this.leftAttributeHashIndex = null;
		this.rightAttributeHashIndex = null;

		
	}
	
	//constructor overloading. just differs by a argument passed from the above method. 
	public JoinOperator(Operator lChild, Operator rChild, List<Expression> tablesJoinClause, Map<String, Integer> leftAttributeHashIndex, Map<String, Integer> rightAttributeHashIndex) {
		this.leftChild = lChild;
		this.rightChild = rChild;
		this.joinExpression = tablesJoinClause;
		this.leftTuple = leftChild.getNextTuple();
		this.rightTuple = null;
		this.leftAttributeHashIndex = leftAttributeHashIndex;
		this.rightAttributeHashIndex = rightAttributeHashIndex;
		
	}

	@Override
	public Tuple getNextTuple() {
		
		
		while(leftTuple!=null) {
			if(rightTuple==null) {
				rightTuple = rightChild.getNextTuple();
			}
			
			while(rightTuple!=null) {
				
				
				if(joinExpression==null) {
					
					Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
					rightTuple = rightChild.getNextTuple();
					return newlyJoinedTuple;	
				}
				else {
					//equijoin
					
					//extract the table name and column name for both tables. match them to see if they are in order.. 
					//i mean say first table is student but in the join clause it can be cpurse.x = student.x, in that case just swap the conditions
					
					if(joinExpression.size()==1) {
						//that means there is just one condition to join the table
					
						ComparisonOperator evalExp = (ComparisonOperator) joinExpression.get(0) ;
					
						String leftExpressionString = evalExp.getLeftExpression().toString();
						String[] splitLeftExpr = leftExpressionString.split("\\."); 
					
						String rightExpressionString = evalExp.getRightExpression().toString();
						String[] splitRightExpr = rightExpressionString.split("\\."); 
					
						String leftCol = splitLeftExpr[1];
						String rightCol = splitRightExpr[1];
					
					
					
						if (compareValues(evalExp, leftExpressionString, rightExpressionString)) {
							Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
							rightTuple = rightChild.getNextTuple();
							return newlyJoinedTuple;
						}

						rightTuple = rightChild.getNextTuple();

					
					//i am filtering and sending the condition to this class only if it is for the two tables that are being sent.
					//hence here, i need not check if the conditions are matching to the tables. 
					//only thing i need to check is if the order of conditions in join matches the tables 
					

					
					//assume the order matches. so the lefttuple belongs to leftAttributeHashIndex and so on.	

					}
					else {
						//this means there will be more than 1 clause to join two tables
						//how will 2 simultaneous conditions to join the same table work... like R.a=S.A and R.B = S.B
						int flag=1;
						for(Expression e: joinExpression) {
							ComparisonOperator evalExp = (ComparisonOperator) e ;
							String leftExpressionString = evalExp.getLeftExpression().toString();
							String rightExpressionString = evalExp.getRightExpression().toString();

							if(compareValues(evalExp, leftExpressionString, rightExpressionString)) {
								flag=flag*1;
							}
							else {
								flag=0; 
								break;
							}
						}
						if(flag==1) {
							//meaning all the conditions for join is satisfied for these two tuples
							Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
							rightTuple = rightChild.getNextTuple();
							return newlyJoinedTuple;
						}
						rightTuple = rightChild.getNextTuple();
					}
				}
			}	
			rightChild.reset();
			leftTuple = leftChild.getNextTuple();	
		}
		return null;
	}
	
	private boolean compareValues(ComparisonOperator operator, String leftCol, String rightCol) {
		Comparable leftValue = leftTuple.get(leftAttributeHashIndex.get(leftCol.toLowerCase()));
		Comparable rightValue = rightTuple.get(rightAttributeHashIndex.get(rightCol.toLowerCase()));
		
		if (operator instanceof EqualsTo) {
	        return leftValue.compareTo(rightValue) == 0;
	    } else if (operator instanceof NotEqualsTo) {
	        return leftValue.compareTo(rightValue) != 0;
	    } else if (operator instanceof GreaterThan) {
	        return leftValue.compareTo(rightValue) > 0;
	    } else if (operator instanceof GreaterThanEquals) {
	        return leftValue.compareTo(rightValue) >= 0;
	    } else if (operator instanceof MinorThanEquals) {
	        return leftValue.compareTo(rightValue) <= 0;
	    } else if (operator instanceof MinorThan) {
	        return leftValue.compareTo(rightValue) < 0;
	    }

	    return false;
		
	}
	
	public Tuple concatenateTuples(Tuple lTup, Tuple rTup) {
		
		Tuple joinedTuple = new Tuple();
		joinedTuple.addTuple(lTup);
		joinedTuple.addTuple(rTup);
		
		return joinedTuple;
					
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		return leftAttributeHashIndex;
		// TODO Auto-generated method stub
		//return attributeHashIndex;
	}

	@Override
	protected String getTableName() {
		// TODO Auto-generated method stub
		
		return null;
	}

}
