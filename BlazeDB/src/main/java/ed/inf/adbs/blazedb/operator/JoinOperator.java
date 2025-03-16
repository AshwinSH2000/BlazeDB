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

public class JoinOperator extends Operator{
	
	private Operator leftChild;
	private Operator rightChild;
	private Expression joinExpression;
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
	
	//function overloading. just differs by a argument passed from the above method. 
	public JoinOperator(Operator lChild, Operator rChild, Expression tablesJoinClause, Map<String, Integer> leftAttributeHashIndex, Map<String, Integer> rightAttributeHashIndex) {
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
		
		//begin going through the left table in outer loop. 
		//in the inside loop, iterate over the right table/child
		
		//not sure about while loop.. but lets see
		
		//have to add the equi join condition here itself. 
		
		while(leftTuple!=null) {
			if(rightTuple==null) {
				rightTuple = rightChild.getNextTuple();
			}
			
			while(rightTuple!=null) {
				
				
				if(joinExpression==null) {
					
					Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
					System.out.println("Left Tuple: ................. "+leftTuple.toString());
					System.out.println("Right Tuple: ................. "+rightTuple.toString());

					System.out.println("Concatenated Tuple: ................. "+newlyJoinedTuple.toString());

					rightTuple = rightChild.getNextTuple();
					return newlyJoinedTuple;	
				}
				else {
					//equijoin
					//need to extract the col from attributehashindex..so i need to pass that one..done passed
					
					//extract the table name and column name for both tables. match them to see if they are in order.. 
					//i mean say first table is student but in the join clause it can be cpurse.x = student.x, in that case just swap the conditions
					ComparisonOperator e = (ComparisonOperator) joinExpression;
					
					String leftExpressionString = e.getLeftExpression().toString();
					String[] splitLeftExpr = leftExpressionString.split("\\."); 
					
					String rightExpressionString = e.getRightExpression().toString();
					String[] splitRightExpr = rightExpressionString.split("\\."); 
					
					String leftCol = splitLeftExpr[1];
					String rightCol = splitRightExpr[1];
					
					
					
					if (compareValues(e, leftExpressionString, rightExpressionString)) {
	                    Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
	                    rightTuple = rightChild.getNextTuple();
	                    return newlyJoinedTuple;
	                }
//					
//					if(joinExpression instanceof EqualsTo) {
//						e = (EqualsTo) joinExpression;
//						if(leftTuple.get(leftAttributeHashIndex.get(leftCol))==rightTuple.get(rightAttributeHashIndex.get(rightCol))) {
//							Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
//							rightTuple = rightChild.getNextTuple();
//							return newlyJoinedTuple;
//						}
////						else {
////							rightTuple = rightChild.getNextTuple();
////							System.out.println("dvanced to next tuple because no match broooooooooooooooooooooooooooo");
////
////						}
//
//					}
//					if(joinExpression instanceof NotEqualsTo) {
//						e = (NotEqualsTo) joinExpression;
//						if(leftTuple.get(leftAttributeHashIndex.get(leftCol))!=rightTuple.get(rightAttributeHashIndex.get(rightCol))) {
//							Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
//							rightTuple = rightChild.getNextTuple();
//							return newlyJoinedTuple;
//						}
//
//					}
//					if(joinExpression instanceof GreaterThan) {
//						e = (GreaterThan) joinExpression;
//						if(leftTuple.get(leftAttributeHashIndex.get(leftCol))>rightTuple.get(rightAttributeHashIndex.get(rightCol))) {
//							Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
//							rightTuple = rightChild.getNextTuple();
//							return newlyJoinedTuple;
//						}
//
//					}
//					if(joinExpression instanceof GreaterThanEquals) {
//						e = (GreaterThanEquals) joinExpression;
//						if(leftTuple.get(leftAttributeHashIndex.get(leftCol))>=rightTuple.get(rightAttributeHashIndex.get(rightCol))) {
//							Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
//							rightTuple = rightChild.getNextTuple();
//							return newlyJoinedTuple;
//						}
//
//					}
//					if(joinExpression instanceof MinorThanEquals) {
//						e = (MinorThanEquals) joinExpression;
//						if(leftTuple.get(leftAttributeHashIndex.get(leftCol))<=rightTuple.get(rightAttributeHashIndex.get(rightCol))) {
//							Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
//							rightTuple = rightChild.getNextTuple();
//							return newlyJoinedTuple;
//						}
//
//					}
//					if(joinExpression instanceof MinorThan) {
//						e = (MinorThan) joinExpression;
//						if(leftTuple.get(leftAttributeHashIndex.get(leftCol))<rightTuple.get(rightAttributeHashIndex.get(rightCol))) {
//							Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
//							rightTuple = rightChild.getNextTuple();
//							return newlyJoinedTuple;
//						}
//
//					}
					rightTuple = rightChild.getNextTuple();

					
					//i am filtering and sending the condition to this class only if it is for the two tables that are being sent.
					//hence here, i need not check if the conditions are matching to the tables. 
					//only thing i need to check is if the order of conditions in join matches the tables 
					
					System.out.println(leftAttributeHashIndex.toString());
					System.out.println(rightAttributeHashIndex.toString());
					System.out.println(joinExpression.toString());
					
					//assume the order matches. so the lefttuple belongs to leftAttributeHashIndex and so on.
					
					
					//String[] leftCol = e.getLeftExpression().toString().split(".");
					//String[] rightCol = e.getRightExpression().toString().split(".");
					
					
					
//					if (splitLeftExpr.length > 1) {
//					    String leftCol = splitLeftExpr[1];  // the second part is the column name
//					    System.out.println(""+leftCol);  
//					} else {
//					    System.out.println("No column name found.");
//					}
					
					System.out.println("kjsdhksjfhfshdkfvsf + leftcol = "+leftCol);
					System.out.println("kjsdhksjfhfshdkfvsf + rightCol = "+rightCol);

					//rightTuple = rightChild.getNextTuple();
					
//					if(leftTuple.get(leftAttributeHashIndex.get(leftCol))==rightTuple.get(rightAttributeHashIndex.get(rightCol))) {
//						Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
//						rightTuple = rightChild.getNextTuple();
//						System.out.println("Called the rightnxt tuple broooooooooooooooooooooooooooo");
//
//						return newlyJoinedTuple;
//					}
//					else {
//						rightTuple = rightChild.getNextTuple();
//						System.out.println("dvanced to next tuple because no match broooooooooooooooooooooooooooo");
//
//					}
					

				}

			}
			
			rightChild.reset();
			System.out.println("Called the right reset  broooooooooooooooooooooooooooo");

			leftTuple = leftChild.getNextTuple();
			System.out.println("Called the leftnet tuple broooooooooooooooooooooooooooo");

			
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
