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
        
        System.out.println("Inside the AndExpression...for this qiery this shuld not be displayed");
    }
    
    
//    @Override
//	public void visit(BinaryExpression expr) {
//        expr.getLeftExpression().accept(this);
//        int leftValue = Integer.parseInt(currentValue); // Extract left operand
//
//        expr.getRightExpression().accept(this);
//        int rightValue = Integer.parseInt(currentValue); // Extract right operand
//
//        // Perform comparison based on the operator
//        if (expr instanceof EqualsTo) {
//            result = (leftValue == rightValue);
//        } else if (expr instanceof GreaterThan) {
//            result = (leftValue > rightValue);
//        } else if (expr instanceof MinorThan) {
//            result = (leftValue < rightValue);
//        }
//        System.out.println("Inside the BinaryExpression...for this query this SHOULD be displayed");
//
//    }
    
    
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
    	
    	System.out.println("binary expression result = "+result);
    }
    
    public void processBinaryExpression(GreaterThanEquals expr) {
    	expr.getLeftExpression().accept(this);
    	int leftValue = Integer.parseInt(currentValue); // left operand

    	expr.getRightExpression().accept(this);
    	int rightValue = Integer.parseInt(currentValue); //  right operand
    	
    	result = (leftValue >= rightValue);
    	
    	System.out.println("binary expression result = "+result);
    }
    
    public void processBinaryExpression(MinorThanEquals expr) {
    	expr.getLeftExpression().accept(this);
    	int leftValue = Integer.parseInt(currentValue); // left operand

    	expr.getRightExpression().accept(this);
    	int rightValue = Integer.parseInt(currentValue); //  right operand
    	
    	result = (leftValue <= rightValue);
    	
    	System.out.println("binary expression result = "+result);
    }
    
    public void processBinaryExpression(NotEqualsTo expr) {
    	expr.getLeftExpression().accept(this);
    	int leftValue = Integer.parseInt(currentValue); // left operand

    	expr.getRightExpression().accept(this);
    	int rightValue = Integer.parseInt(currentValue); //  right operand
    	
    	result = (leftValue != rightValue);
    	
    	System.out.println("binary expression result = "+result);
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
		
		System.out.println("THE COLUMN INSIDE THE VISIT FUNCTION IS "+column);
		
		//commented the below two lines because i now started using tableName.columnName as keys in attributeHashIndex
        //String columnName = column.getColumnName();
		//System.out.println("THE COLUMNNAME INSIDE THE VISIT FUNCTION IS "+columnName);

        if (attributeHashIndex.containsKey(column.toString().toLowerCase())) {
            currentValue = tuple.get(   attributeHashIndex.get(   column.toString().toLowerCase()   )).toString();
            System.out.println("The current column value is ASHWINSH MAN "+currentValue);
        }
        
    }

    @Override
    public void visit(LongValue longValue) {
        currentValue = String.valueOf(longValue.getValue());
        System.out.println("Inside the visit(long)...identakeee "+ currentValue);

    }

    public boolean evaluate(Expression whereCondition) {
        whereCondition.accept(this); // Start traversal
        return result; // Return evaluation result
                
    }
    
}
