package ed.inf.adbs.blazedb.operator;

import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/*
 * This is a class that is specifically used to evaluate the expression following the WHERE clause
 * It parses the passed WHERE condition and splits it with respect to the AND keyword. 
 * Then it evaluates the left and right expressions based on the type of operator present in it. 
 * Finally calculates the boolean value and returns from the evaluate() method.  
 */
public class EvaluateSelection extends ExpressionDeParser{

	private Map<String, Integer> attributeHashIndex; //it maps column names to their index in the tuple
	private Tuple tuple;   // The tuple being checked
	private boolean result; // Stores whether the condition is satisfied
    private String currentValue; // Temporary storage for extracted values


    /*
     * Constructor used to initialize the class variables
     * @param attributeHashIndex The HashMap that contains a mapping from columnNames to the integer representing column's position in the tuple
     * @param tuple The actual tuple on which to evaluate the WHERE condition
     */
	public EvaluateSelection(Map<String, Integer> attributeHashIndex,Tuple tuple ) {
		this.attributeHashIndex = attributeHashIndex;
		this.tuple = tuple;
		
		//Going with false by default.
		this.result = false; 		
	}
	
	
	/*
	 * Method to split the main WHERE clause around AND keyword. 
	 * It works recursively in case there are more than two AND keywords
	 * 
	 * @param expr The Expression containing WHERE clause to evaluate
	 */
    @Override
    public void visit(AndExpression expr) {
        expr.getLeftExpression().accept(this);
        boolean leftResult = result; // Store result of left condition

        expr.getRightExpression().accept(this);
        boolean rightResult = result; // Store result of right condition

        // Apply AND logic
        result = leftResult && rightResult;
        
    }

    /*
     * Method to evaluate the binary expression based on the operator present in the expression and store it in result
     * 
     * @param expr BinaryExpression that needs to be evaluated
     */
    public void processBinaryExpression(BinaryExpression expr) {
    	expr.getLeftExpression().accept(this);
    	int leftValue = Integer.parseInt(currentValue); // left operand

    	expr.getRightExpression().accept(this);
    	int rightValue = Integer.parseInt(currentValue); //  right operand
    	
    	if (expr instanceof EqualsTo) {
        	result = (leftValue == rightValue);
        } else if (expr instanceof GreaterThan) {
            result = (leftValue > rightValue);
        } else if (expr instanceof MinorThan) {
            result = (leftValue < rightValue);
        } else if (expr instanceof GreaterThanEquals) {
            result = (leftValue >= rightValue);
        } else if (expr instanceof MinorThanEquals) {
            result = (leftValue <= rightValue);
        } else if (expr instanceof NotEqualsTo) {
            result = (leftValue != rightValue);
        }
    }
    
    /*
     * Method that gets called if the expression contains '==' between two operands.
     * @param expr The expression that needs to be evaluated
     */
    @Override
    public void visit(EqualsTo expr) {
        processBinaryExpression(expr);
    }
    
    
    /*
     * Method that gets called if the expression contains '>' between two operands.
     * @param expr The expression that needs to be evaluated
     */
    @Override
    public void visit(GreaterThan expr) {
        processBinaryExpression(expr);
    }

    
    /*
     * Method that gets called if the expression contains '<' between two operands.
     * @param expr The expression that needs to be evaluated
     */
    @Override
    public void visit(MinorThan expr) {
        processBinaryExpression(expr);
    }
    
    
    /*
     * Method that gets called if the expression contains '>=' between two operands.
     * @param expr The expression that needs to be evaluated
     */
    @Override
    public void visit(GreaterThanEquals expr) {
    	processBinaryExpression(expr);
    }
    
    
    /*
     * Method that gets called if the expression contains '<=' between two operands.
     * @param expr The expression that needs to be evaluated
     */
    @Override
    public void visit(MinorThanEquals expr) {
    	processBinaryExpression(expr);
    }
    
    
    /*
     * Method that gets called if the expression contains '!=' between two operands.
     * @param expr The expression that needs to be evaluated
     */
    @Override
    public void visit(NotEqualsTo expr) {
    	processBinaryExpression(expr);
    }
	
    
    /*
     * Method that gets called when the left or right operand is of the type of a column reference. Its value need to be extracted. 
     * @param column The column whose integer value needs to be extracted and stored in currentValue variable. 
     */
	@Override
    public void visit(Column column) {
        if (attributeHashIndex.containsKey(column.toString().toLowerCase())) {
            currentValue = tuple.get(   attributeHashIndex.get(   column.toString().toLowerCase()   )).toString();
        }
    }

	
	/*
     * Method that gets called when the left or right operand is of the type of a Long number.  
     * @param column The column whose  value needs to be stored in currentValue variable. 
     */
    @Override
    public void visit(LongValue longValue) {
        currentValue = String.valueOf(longValue.getValue());
        
    }

    
    /*
     * Method to begin the parsing of the WHERE clause. 
     * From this method it jumps to various 'visit' method based on the expression. 
     * 
     *  @param whereCondition The entire WHERE condition passed from the SQL input. 
     *  @return result The boolean result after evaluating the WHERE clause.
     */
    public boolean evaluate(Expression whereCondition) {
    	// Start traversal using visit methods
        whereCondition.accept(this); 
        
        return result; 
                
    }
    
}
