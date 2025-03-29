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

public class EvaluateSelection extends ExpressionDeParser{

	private Map<String, Integer> attributeHashIndex; //it maps column names to their index in the tuple
	private Tuple tuple;   // The tuple being checked
	private boolean result; // Stores whether the condition is satisfied
	
	public EvaluateSelection(Map<String, Integer> attributeHashIndex,Tuple tuple ) {
		this.attributeHashIndex = attributeHashIndex;
		this.tuple = tuple;
		
		//going by false by default.
		this.result = false; 		
	}
	
    private String currentValue; // Temporary storage for extracted values
	
    @Override
    public void visit(AndExpression expr) {
        expr.getLeftExpression().accept(this);
        boolean leftResult = result; // Store result of left condition

        expr.getRightExpression().accept(this);
        boolean rightResult = result; // Store result of right condition

        // Apply AND logic
        result = leftResult && rightResult;
        
    }
    
    
    
    
    public void processBinaryExpression(EqualsTo expr) {
    	expr.getLeftExpression().accept(this);
    	int leftValue = Integer.parseInt(currentValue); // Extract left operand

    	expr.getRightExpression().accept(this);
    	int rightValue = Integer.parseInt(currentValue); // Extract right operand
    	
    	result = (leftValue == rightValue);
    }
    
    
    public void processBinaryExpression(GreaterThan expr) {
    	expr.getLeftExpression().accept(this);
    	int leftValue = Integer.parseInt(currentValue); // Extract left operand

    	expr.getRightExpression().accept(this);
    	int rightValue = Integer.parseInt(currentValue); // Extract right operand
    	
    	result = (leftValue > rightValue);
    }
    
    public void processBinaryExpression(MinorThan expr) {
    	expr.getLeftExpression().accept(this);
    	int leftValue = Integer.parseInt(currentValue); // Extract left operand

    	expr.getRightExpression().accept(this);
    	int rightValue = Integer.parseInt(currentValue); // Extract right operand
    	
    	result = (leftValue < rightValue);
    	
    }
    
    public void processBinaryExpression(GreaterThanEquals expr) {
    	expr.getLeftExpression().accept(this);
    	int leftValue = Integer.parseInt(currentValue); // left operand

    	expr.getRightExpression().accept(this);
    	int rightValue = Integer.parseInt(currentValue); //  right operand
    	
    	result = (leftValue >= rightValue);
    	
    }
    
    public void processBinaryExpression(MinorThanEquals expr) {
    	expr.getLeftExpression().accept(this);
    	int leftValue = Integer.parseInt(currentValue); // left operand

    	expr.getRightExpression().accept(this);
    	int rightValue = Integer.parseInt(currentValue); //  right operand
    	
    	result = (leftValue <= rightValue);
    	
    }
    
    public void processBinaryExpression(NotEqualsTo expr) {
    	expr.getLeftExpression().accept(this);
    	int leftValue = Integer.parseInt(currentValue); // left operand

    	expr.getRightExpression().accept(this);
    	int rightValue = Integer.parseInt(currentValue); //  right operand
    	
    	result = (leftValue != rightValue);
    	
    }

    
    @Override
    public void visit(EqualsTo expr) {
        processBinaryExpression(expr);
    }
    @Override
    public void visit(GreaterThan expr) {
        processBinaryExpression(expr);
    }

    @Override
    public void visit(MinorThan expr) {
        processBinaryExpression(expr);
    }
    
    @Override
    public void visit(GreaterThanEquals expr) {
    	processBinaryExpression(expr);
    }
    
    @Override
    public void visit(MinorThanEquals expr) {
    	processBinaryExpression(expr);
    }
    
    @Override
    public void visit(NotEqualsTo expr) {
    	processBinaryExpression(expr);
    }
	
	@Override
    public void visit(Column column) {
		

        if (attributeHashIndex.containsKey(column.toString().toLowerCase())) {
            currentValue = tuple.get(   attributeHashIndex.get(   column.toString().toLowerCase()   )).toString();
        }
        
    }

    @Override
    public void visit(LongValue longValue) {
        currentValue = String.valueOf(longValue.getValue());

    }

    public boolean evaluate(Expression whereCondition) {
        whereCondition.accept(this); // Start traversal
        return result; // Return evaluation result
                
    }
    
}
