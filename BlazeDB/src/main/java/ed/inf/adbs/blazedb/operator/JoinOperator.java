package ed.inf.adbs.blazedb.operator;

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
 * If there is no where clause, it simply concatenates both the tuples it is currently focusing on.
 * If there is a where clause, it join the two tuples it is focusing on, if the conditions mentioned match for them.
 * 
 */
public class JoinOperator extends Operator {

	private Operator leftChild;
	private Operator rightChild;
	private List<Expression> joinExpression;
	private Tuple leftTuple;
	private Tuple rightTuple;
	private Map<String, Integer> leftAttributeHashIndex;
	private Map<String, Integer> rightAttributeHashIndex;

	/*
	 * Constructor for the joinOperator
	 * 
	 * @param lChild This is the left child (table) to be joined
	 * 
	 * @param rChild This is the right child (table) to be joined
	 */
	public JoinOperator(Operator lChild, Operator rChild) {
		this.leftChild = lChild;
		this.rightChild = rChild;
		this.joinExpression = null;
		this.leftTuple = leftChild.getNextTuple();
		this.rightTuple = null;
		this.leftAttributeHashIndex = null;
		this.rightAttributeHashIndex = null;
	}

	/*
	 * This too is a constructor for joinOperator - constructor overloading as it
	 * just differs by a arguments passed from the above constructor.
	 * 
	 * @param lChild This is the left child (table) to be joined
	 * 
	 * @param rChild This is the right child (table) to be joined
	 * 
	 * @param tableJoinClause This is a List of Expressions that need to match for
	 * the tables to be joined
	 * 
	 * @param leftAttributeHashIndex HashMap that maps table attributes to the
	 * integer representing its position in the tuple of left child
	 * 
	 * @param rightAttributeHashIndex HashMap that maps table attributes to the
	 * integer representing its position in the tuple of right child
	 */
	public JoinOperator(Operator lChild, Operator rChild, List<Expression> tablesJoinClause,
			Map<String, Integer> leftAttributeHashIndex, Map<String, Integer> rightAttributeHashIndex) {
		this.leftChild = lChild;
		this.rightChild = rChild;
		this.joinExpression = tablesJoinClause;
		this.leftTuple = leftChild.getNextTuple();
		this.rightTuple = null;
		this.leftAttributeHashIndex = leftAttributeHashIndex;
		this.rightAttributeHashIndex = rightAttributeHashIndex;
	}

	/*
	 * Method that scans each and every pair of tuples from both tables and joins
	 * them based on some conditions. If conditions are present and applicable for
	 * these tables, they need to be satisfied to join them (equijoin) If conditions
	 * are not present, it performs a cross join of the two tables
	 * 
	 */
	@Override
	public Tuple getNextTuple() {

		while (leftTuple != null) {
			if (rightTuple == null) {
				rightTuple = rightChild.getNextTuple();
			}

			while (rightTuple != null) {

				if (joinExpression == null) {

					// cross join
					Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
					rightTuple = rightChild.getNextTuple();
					return newlyJoinedTuple;
				} else {
					// equijoin
					if (joinExpression.size() == 1) {
						// There is just one condition to join the table

						ComparisonOperator evalExp = (ComparisonOperator) joinExpression.get(0);
						String leftExpressionString = evalExp.getLeftExpression().toString();
						String rightExpressionString = evalExp.getRightExpression().toString();

						// I am filtering and sending the condition to this class only if it is for the
						// two tables that are being sent.
						// Hence here, I am not checking if the conditions are matching to the tables.
						// I am just checking if the values of conditions are matching.
						if (compareValues(evalExp, leftExpressionString, rightExpressionString)) {
							Tuple newlyJoinedTuple = concatenateTuples(leftTuple, rightTuple);
							rightTuple = rightChild.getNextTuple();
							return newlyJoinedTuple;
						}

						rightTuple = rightChild.getNextTuple();

					} else {
						// There will be more than 1 clause to join two tables
						// Iterating through the joinExpression to check for all the clauses before
						// joining.
						int flag = 1;
						for (Expression e : joinExpression) {

							ComparisonOperator evalExp = (ComparisonOperator) e;
							String leftExpressionString = evalExp.getLeftExpression().toString();
							String rightExpressionString = evalExp.getRightExpression().toString();

							if (compareValues(evalExp, leftExpressionString, rightExpressionString)) {
								flag = flag * 1;
							} else {
								// In case it comes here, it means that one of the conditions to join these two
								// tuples is false.
								// Hence we should not join them.
								flag = 0;
								break;
							}
						}

						if (flag == 1) {
							// Meaning all the conditions for join is satisfied for these two tuples
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

	/*
	 * Method to compare the values of two tuples based on the operator between them
	 * 
	 * @param operator It is actually the joinExpression passed in the form of a
	 * ComparisonOperator.
	 * 
	 * @param leftCol the left operand in the join expression
	 * 
	 * @param rightCol the right operand in the join expression
	 * 
	 * @return (boolean value) The boolean result of evaluating the expression using
	 * values from left and right operands
	 */
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

	/*
	 * Method to concatenate two tuples (join two tuples)
	 * 
	 * @param lTup The left tuple (list of integers)
	 * 
	 * @param rTup The right tuple (list of integers) to be joined to the left tuple
	 * 
	 * @return joinedTuple The result obtained after the right tuple is
	 * concatenated/joined to the left tuple
	 */
	public Tuple concatenateTuples(Tuple lTup, Tuple rTup) {

		Tuple joinedTuple = new Tuple();
		joinedTuple.addTuple(lTup);
		joinedTuple.addTuple(rTup);
		return joinedTuple;
	}

	/*
	 * Method to reset the rightChild (inner table)
	 */
	@Override
	public void reset() {
		rightChild.reset();
	}

	/*
	 * This is a method used to return the left child's HashMap that contains a
	 * mapping from table attributes to integers representing the respective
	 * column's position in the tuple.
	 * 
	 * @return leftAttributeHashIndex HashMap that maps left child's attributes to
	 * integers representing its position.
	 */
	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		return leftAttributeHashIndex;
	}

	/*
	 * Method used to return the left child's tableName
	 * 
	 * @return The name of the table the root of this operator is referring to.
	 */
	@Override
	protected String getTableName() {
		return leftChild.getTableName();
	}

}
