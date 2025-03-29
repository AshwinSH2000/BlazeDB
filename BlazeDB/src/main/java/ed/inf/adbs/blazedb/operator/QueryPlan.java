package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.DatabaseCatalog;
//import ed.inf.adbs.blazedb.ExpressionCombiner;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;


public class QueryPlan {
	
	/*
	 * @param - all the parsed pieces of sql code, NEED TO CHECK if i need to pass anymore.
	 * @return - nothing, as of now. but might need to change it to be Operator I think, not sure. 
	 */
	
	public static Operator buildQueryPlan(List<SelectItem<?>> SELECT, Distinct DISTINCT, List<OrderByElement> ORDERBY,
									  ExpressionList GROUPBY, Expression WHERE, List<Join> JOIN, FromItem FROM) {
		
		DatabaseCatalog catalog = DatabaseCatalog.getInstance();
		
		
		if(JOIN==null) {
			
			//this ensures that there is only 1 table because fromItem picks up only one table
			//and moreover if join is null, then it just means 1 talbe. 
			Operator root=new ScanOperator(FROM.toString());
			Map<String, Integer> attributeHashIndex = root.getAttributeHashIndex();
			//this might give ERROR ERROR later on because i changed the datatype of root from operator to scanoperator.
			
			//now i am adding the from and where but i need to add the join clause sometimes. 
			if(WHERE!=null) {
				root = new SelectionOperator(root, WHERE, attributeHashIndex);
			}
			
			// sorting can be done here as well but it leads to larger tuples being compared which can reduce the speed
			if(!SELECT.toString().contains("[*]")) {
				
				
				if(!SELECT.toString().toLowerCase().contains("sum") && GROUPBY == null) {
					root = new ProjectionOperator(root, SELECT, attributeHashIndex);
					//pulling the new attribute hash index containing the projected columns only
					attributeHashIndex = root.getAttributeHashIndex();
				}
				
				else
				{
					root = new SumOperator(root, GROUPBY, SELECT, attributeHashIndex);
					attributeHashIndex = root.getAttributeHashIndex();
				}
				
			}
			
			if(DISTINCT!=null) {
				root = new DuplicateEliminationOperator(root);
			}
			
			if(ORDERBY!=null) {
				root = new SortOperator(root, ORDERBY, attributeHashIndex);	
			}

			return root;
			
		} //end of if join==null
		
		if(JOIN!=null) {
			Operator leftChild = new ScanOperator(FROM.toString());
			Map<String, Integer> attributeHashIndex_lChild = leftChild.getAttributeHashIndex();
			String leftTableName = FROM.toString();

			
			//split the where clause	
			
			List<Expression> listExp = splitExpression(WHERE);
			//Splits it in the form of exp1 <op> exp2, exp3 <op> exp4 etc.
			//do the bwlow only if where clasue is not null...need to add that clie
			if(!(WHERE==null))
			{
				//System.out.println("Since there is no where clause in this sql query, i will not include the below block of statemets");
				
				List<Expression> listTableOneClause=conditionForTable(listExp, leftTableName);
				if(!listTableOneClause.isEmpty()) {
					Expression tableOneClause = combineWithAnd(listTableOneClause);
					leftChild = new SelectionOperator(leftChild, tableOneClause, attributeHashIndex_lChild);

				}

			}
			
			
			//next have to verify for each table if a projection is valid for it
			//function tha ttakes tablename, listExp ... checks each exp if there is a clause matching it. 
			//if matching, it returns the clause else null
			
			
			for( Join join : JOIN) {
				//System.out.println("join var = "+join.toString());
				
				//need to extract the join clause. see the optimal methods. 

				//this loop is to iteratively handle all the tables in JOIN but as of not it is just focussing on one table. 
				//maybe create a List of Joins again to have multiple tables on the fly
				
				
				//Map<String, Integer> joinedTableAttributes = leftChild.getAttributeHashIndex();
				
				Operator rightChild = new ScanOperator(join.getRightItem().toString());
				Map<String, Integer> attributeHashIndex_rChild = rightChild.getAttributeHashIndex();
				
				if(!(WHERE==null)) {
					
					List<Expression> listTableTwoClause=conditionForTable(listExp, join.toString());
					if(!listTableTwoClause.isEmpty())
					{
						Expression tableTwoClause = combineWithAnd(listTableTwoClause);

						rightChild = new SelectionOperator(rightChild, tableTwoClause, attributeHashIndex_rChild);	
					}
				}
				

				//need to refine this sentence. In the first itr, its ok to have FROM and join. but in all subsequent itrs, you need pass joined
				//tables and the join
				
				if(!(WHERE==null)) {
					List<Expression> listTablesJoinClause = conditionsForTwoTables(listExp,leftTableName,join.toString());
					if(!listTablesJoinClause.isEmpty()) {
						
						Expression tablesJoinClause =  combineWithAnd(listTablesJoinClause);
						//leftChild = new JoinOperator(leftChild, rightChild, tablesJoinClause, attributeHashIndex_lChild, attributeHashIndex_rChild );
						leftChild = new JoinOperator(leftChild, rightChild, listTablesJoinClause, attributeHashIndex_lChild, attributeHashIndex_rChild );

					}
				
					else {
						//System.out.println("Joining the two tables with cross product because of no where join clause present. #################### ");
						leftChild = new JoinOperator(leftChild, rightChild);
					}
				}	
				else {
					//System.out.println("Joining the two tables with cross product because of no where join clause present. #################### ");
					leftChild = new JoinOperator(leftChild, rightChild);
				}
				
				int offset=attributeHashIndex_lChild.size();
				
				//this is to ensure that for any subsequent joins, the updated attHashIndex is sent
				for (Map.Entry<String, Integer> entry : attributeHashIndex_rChild.entrySet()) {
				    //String newKey = join.getRightItem().toString() + "." + entry.getKey(); // Prefix with table name
					if(!attributeHashIndex_lChild.containsKey(entry.getKey()))
						attributeHashIndex_lChild.put(entry.getKey(), entry.getValue() + offset);
				}
				leftTableName = leftTableName.concat(" join "+join.toString());
			
			} //join loop ends

			
			
				
			// sorting can be done here as well but it leads to larger tuples being compared which can reduce the speed
			if(!SELECT.toString().contains("[*]")) {
				if(!SELECT.toString().toLowerCase().contains("sum") && GROUPBY == null) {
					leftChild = new ProjectionOperator(leftChild, SELECT, attributeHashIndex_lChild);
					//pulling the new attribute hash index containing the projected columns only
					attributeHashIndex_lChild = leftChild.getAttributeHashIndex();
				}
				else
				{
					leftChild = new SumOperator(leftChild, GROUPBY, SELECT, attributeHashIndex_lChild);
					attributeHashIndex_lChild = leftChild.getAttributeHashIndex();
				}
				
			}
			
			
			
			if(DISTINCT!=null) {
				leftChild = new DuplicateEliminationOperator(leftChild);
			}
			
			if(ORDERBY!=null) {
				leftChild = new SortOperator(leftChild, ORDERBY, attributeHashIndex_lChild);
			}
			return leftChild;
			
		}
		return null;
		
	}
	
	
	private static List<Expression> conditionsForTwoTables(List<Expression> listExp, String tableOne, String tableTwo){
		
		List<Expression> returnClause = new ArrayList<>();
		
		for(Expression exp : listExp) {
			ComparisonOperator e = null;
			
			if (exp instanceof GreaterThan)
			{
				 //GreaterThan e = (GreaterThan) exp;
				e = (GreaterThan) exp;
			}
			
			if (exp instanceof MinorThan)
			{
				e = (MinorThan) exp;
			}
			
			if (exp instanceof GreaterThanEquals)
			{
				e = (GreaterThanEquals) exp;
			}
			
			if (exp instanceof MinorThanEquals)
			{
				e = (MinorThanEquals) exp;
			}
			
			if (exp instanceof EqualsTo)
			{
				e = (EqualsTo) exp;
			}
			
			if (exp instanceof NotEqualsTo)
			{
				e = (NotEqualsTo) exp;
			}
			
			//cases to handle
			//1. tbl1.col <OP> tbl2.col
			//2. tbl2.col <OP> tbl1.col
			

			

			if(tableOne.contains("join")) {
				
				String[] leftTableInJoinClause = e.getLeftExpression().toString().toLowerCase().split("\\."); //index 0 will be the name of the table that we wish to check
				String[] rightTableInJoinClause = e.getRightExpression().toString().toLowerCase().split("\\.");
				
				if (   (tableOne.toLowerCase().contains(leftTableInJoinClause[0].toLowerCase()) 
					    &&
				       tableTwo.toLowerCase().contains(rightTableInJoinClause[0].toLowerCase()))   
					   ||
					   (tableOne.toLowerCase().contains(rightTableInJoinClause[0].toLowerCase()) 
					    &&
						tableTwo.toLowerCase().contains(leftTableInJoinClause[0].toLowerCase())   )
					)
				{
					if((tableOne.toLowerCase().contains(leftTableInJoinClause[0].toLowerCase()) 
					    &&
				       tableTwo.toLowerCase().contains(rightTableInJoinClause[0].toLowerCase())))
					{
						returnClause.add(e);
					}
					else if((tableOne.toLowerCase().contains(rightTableInJoinClause[0].toLowerCase()) 
						    &&
							tableTwo.toLowerCase().contains(leftTableInJoinClause[0].toLowerCase())   ))
					{
						returnClause.add(reorderClause(e));
					}
				}
				
				
			}
			else {

			if
			(
				(e.getLeftExpression().toString().toLowerCase().contains(tableOne.toLowerCase())
				&& e.getRightExpression().toString().toLowerCase().contains(tableTwo.toLowerCase()) )
				||
				(e.getLeftExpression().toString().toLowerCase().contains(tableTwo.toLowerCase())
				&& e.getRightExpression().toString().toLowerCase().contains(tableOne.toLowerCase()))
						
				
			)					

			{
				if(   e.getLeftExpression().toString().toLowerCase().contains(tableOne.toLowerCase())
						&& e.getRightExpression().toString().toLowerCase().contains(tableTwo.toLowerCase())    ) 
				{
					returnClause.add(e);
				}
				else if(  e.getLeftExpression().toString().toLowerCase().contains(tableTwo.toLowerCase())
						&& e.getRightExpression().toString().toLowerCase().contains(tableOne.toLowerCase()) ) {
					//split the clause. reorder it and join it
					
					returnClause.add(reorderClause(e));
				}
			}
			
			}
			
			
		}
		
		return returnClause;
	}
	
	private static ComparisonOperator reorderClause(ComparisonOperator e) {
		ComparisonOperator reversedClause = null;
		
		Expression leftExp = e.getLeftExpression();
		Expression rightExp = e.getRightExpression();
		ComparisonOperator op = null;
		
		if(e instanceof GreaterThan) {
			op = new MinorThan();	
		}
		if(e instanceof MinorThan) {
			op = new GreaterThan();
		}
		if(e instanceof GreaterThanEquals) {
			op = new MinorThanEquals();
		}
		if(e instanceof MinorThanEquals) {
			op = new GreaterThanEquals();
		}
		if(e instanceof EqualsTo) {
			op = new EqualsTo();
		}
		if(e instanceof NotEqualsTo) {
			op = new NotEqualsTo();
		}
		
		op.setLeftExpression(rightExp);
		op.setRightExpression(leftExp);
		reversedClause = op;
		
		return reversedClause;
	}
	
	private static List<Expression> conditionForTable(List<Expression> listExp, String table) {
       
//		if (listExp instanceof BinaryExpression binaryExpr) {
//            Expression left = binaryExpr.getLeftExpression();
//            Expression right = binaryExpr.getRightExpression();
//
//            if (left instanceof Column leftColumn && leftColumn.getTable() != null) {
//                return leftColumn.getTable().getName().equalsIgnoreCase(table);
//            }
//            if (right instanceof Column rightColumn && rightColumn.getTable() != null) {
//                return rightColumn.getTable().getName().equalsIgnoreCase(table);
//            }
//        }
		
		List<Expression> returnClause = new ArrayList<>();
		
		for(Expression exp: listExp) {
		
			ComparisonOperator e = null;
			
			if (exp instanceof GreaterThan)
			{
				 //GreaterThan e = (GreaterThan) exp;
				e = (GreaterThan) exp;
			}
			
			if (exp instanceof MinorThan)
			{
				e = (MinorThan) exp;
			}
			
			if (exp instanceof GreaterThanEquals)
			{
				e = (GreaterThanEquals) exp;
			}
			
			if (exp instanceof MinorThanEquals)
			{
				e = (MinorThanEquals) exp;
			}
			
			if (exp instanceof EqualsTo)
			{
				e = (EqualsTo) exp;
			}
			
			if (exp instanceof NotEqualsTo)
			{
				e = (NotEqualsTo) exp;
			}
			
				
//				here i pass the entire list of where expr and also the table name
//				if i find a clause associated with a table, i thought of returning it directly initially
//				but there can be a case where two clauses are associated with a same table say A.a>5 and A.b<6
//				So i feel I need to create an expression and then return it. 
				

			if //this checks if the where clause for the table is a single table where clause or not
			(		(e.getLeftExpression().toString().toLowerCase().contains(table.toLowerCase())  
					&& e.getRightExpression().toString().toLowerCase().contains(table.toLowerCase()))
					||
					(e.getLeftExpression().toString().toLowerCase().contains(table.toLowerCase())  
					&& !(e.getRightExpression() instanceof Column))
					||
					(e.getRightExpression().toString().toLowerCase().contains(table.toLowerCase())
					&& !(e.getLeftExpression() instanceof Column))
					||
					(isInteger(e.getLeftExpression()) && isInteger(e.getRightExpression()))
						
			) 
			{
				returnClause.add(e);
				
					
			}
				//if(e.getLeftExpression().toString().toLowerCase()) {}
				//if lhs contains rhs, then single tbale exp yes
				//if lhs has the table name and rhs is a constant then yes
				//if rhs is a table name and lhs is a constant then yes
		
		}
        return returnClause;
    }
	
	public static boolean isInteger(Expression exp)
	{
		try {
			Integer.parseInt(exp.toString() );
			return true;
			
		}
		catch(NumberFormatException e) {
			return false;
		}
	}
	
	
	private static List<Expression> splitExpression(Expression expression) {
        List<Expression> expressions = new ArrayList<>();
        
        //if the expression is a logical conjunction and/or then split it recursively
        if (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            
            expressions.addAll(splitExpression(andExpression.getLeftExpression()));
            expressions.addAll(splitExpression(andExpression.getRightExpression()));
        } 
        //the below wont be of much use to this program but added anyway to test custom queries. 
        else if (expression instanceof OrExpression) {
            OrExpression orExpression = (OrExpression) expression;
            
            expressions.addAll(splitExpression(orExpression.getLeftExpression()));
            expressions.addAll(splitExpression(orExpression.getRightExpression()));
        } 
        else {
            // Base case: simple expression
            expressions.add(expression);
        }
        
        return expressions;
    }
	
	private static Expression combineWithAnd(List<Expression> expressions) {
        if (expressions == null || expressions.isEmpty()) {
            return null;  // No expressions to combine
        }

        // Start with the first expression
        Expression combinedExpression = expressions.get(0);

        // Iterate through the list and combine the expressions with AND
        for (int i = 1; i < expressions.size(); i++) {
            combinedExpression = new AndExpression(combinedExpression, expressions.get(i));
        }
        
        //System.out.println("returning ... "+combinedExpression.toString());

        return combinedExpression;
    }
}

	
	