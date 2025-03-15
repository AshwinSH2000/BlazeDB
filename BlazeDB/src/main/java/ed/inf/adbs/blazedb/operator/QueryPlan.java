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
	
	@SuppressWarnings("deprecation")
	public static Operator buildQueryPlan(List<SelectItem<?>> SELECT, Distinct DISTINCT, List<OrderByElement> ORDERBY,
									  GroupByElement GROUPBY, Expression WHERE, List<Join> JOIN, FromItem FROM) {
		
		DatabaseCatalog catalog = DatabaseCatalog.getInstance();
		
		
		
		// IMPORTANT! NOW YOU NEED TO CHECK THE NUMBER OF TABLES BEFORE YOU ASSIGN THE ROOT AS SCANOPERATOR. 
		// IN CASE, THERE IS JUST A TABLE, PROCEED. 
		// ELSE, YOU NEED TO CREATE TWO SEPARATE INSTANCES OF SCANOPERATORS.. NEED TO CHECK THE FROM IN THIS STEP(thatsthe soln)
		
		if(JOIN==null) {
			
			//this ensures that there is only 1 table because fromItem picks up only one table
			//and moreover if join is null, then it just means 1 talbe. 
			Operator root=new ScanOperator(FROM.toString());
			Map<String, Integer> attributeHashIndex = root.getAttributeHashIndex();
			//this might give ERROR ERROR later on because i changed the datatype of root from operator to scanoperator.
			
			//now i am adding the from and where but i need to add the join clause sometimes. 
			if(WHERE!=null) {
				System.out.println("Going to wrap selection operator");
				root = new SelectionOperator(root, WHERE, attributeHashIndex);
			}
			if(!SELECT.toString().contains("[*]")) {
				System.out.println("Detected projection criteria...so root is projectionoperator");
				root = new ProjectionOperator(root, SELECT, attributeHashIndex);
			}
			
			return root;
			
		}
		
		if(JOIN!=null) {
			Operator leftChild = new ScanOperator(FROM.toString());
			Map<String, Integer> attributeHashIndex_lChild = leftChild.getAttributeHashIndex();
			String leftTableName = FROM.toString();
			
			
			//Expression exp = null;

			System.out.println(FROM.toString()+" lallalla "+JOIN.toString());
			
			//split the where clause	
			
			List<Expression> listExp = splitExpression(WHERE);

			//do the bwlow only if where clasue is not null...need to add that clie
			if(!(WHERE==null))
			{
				//System.out.println("Since there is no where clause in this sql query, i will not include the below block of statemets");
				
				List<Expression> listTableOneClause=conditionForTable(listExp, leftTableName);
				if(!listTableOneClause.isEmpty()) {
					System.out.println("hi hello namaste.............found a where clause for table..........." + listTableOneClause.toString());
					Expression tableOneClause = combineWithAnd(listTableOneClause);
					System.out.println("hi hello namaste.............joined expr for above clause..........." + tableOneClause.toString());
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
					System.out.println("I am printing the value of temp2 wen there is no hwere claise: "+listTableTwoClause.toString());
					if(!listTableTwoClause.isEmpty())
					{
						System.out.println("hi hello namaste.............found a where clause for table..........." + listTableTwoClause.toString());
						Expression tableTwoClause = combineWithAnd(listTableTwoClause);
						System.out.println("hi hello namaste.............joined expr for above clause..........." + tableTwoClause.toString());

						rightChild = new SelectionOperator(rightChild, tableTwoClause, attributeHashIndex_rChild);	
					}
				}
				

				//need to refine this sentence. In the first itr, its ok to have FROM and join. but in all subsequent itrs, you need pass joined
				//tables and the join
				
				if(!(WHERE==null)) {
					List<Expression> listTablesJoinClause = conditionsForTwoTables(listExp,leftTableName,join.toString());
					if(!listTablesJoinClause.isEmpty()) {
						
						Expression tablesJoinClause =  combineWithAnd(listTablesJoinClause);
						System.out.println("Printing the single join clause joined with AND operator...lets see the op   "+ tablesJoinClause.toString());
						leftChild = new JoinOperator(leftChild, rightChild, tablesJoinClause, attributeHashIndex_lChild, attributeHashIndex_rChild );

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
				
				System.out.println("before updation lahi is "+attributeHashIndex_lChild.toString());
				System.out.println("before updation rahi is "+attributeHashIndex_rChild.toString());

				//this is to ensure that for any subsequent joins, the updated attHashIndex is sent
				for (Map.Entry<String, Integer> entry : attributeHashIndex_rChild.entrySet()) {
				    //String newKey = join.getRightItem().toString() + "." + entry.getKey(); // Prefix with table name
					if(!attributeHashIndex_lChild.containsKey(entry.getKey()))
						attributeHashIndex_lChild.put(entry.getKey(), entry.getValue() + offset);
				}
				leftTableName = leftTableName.concat(" join "+join.toString());
				
				
				System.out.println("after updation lahi is "+attributeHashIndex_lChild.toString());
				System.out.println("after updation rahi is "+attributeHashIndex_rChild.toString());

				//seems like the queries consist of only simpleJoin (mostly) and InnerJoin. sp going ahead with them for timebeing. 
//				System.out.println("displayng the rightitem: "+join.getRightItem().toString());
//				System.out.println("displaying the isjoin iscross: "+join.isCross() );
//				System.out.println("displaying the isjoin issimple: "+join.isSimple() );
//				System.out.println("displaying the isjoin isapply: "+join.isApply() );
//				System.out.println("displaying the isjoin isfull: "+join.isFull() );
//				System.out.println("displaying the isjoin isinner: "+join.isInner() );
//				System.out.println("displaying the isjoin isinnerjoin: "+join.isInnerJoin() );
//				System.out.println("displaying the isjoin isleft: "+join.isLeft() );
//				System.out.println("displaying the isjoin isnatural: "+join.isNatural() );
//				System.out.println("displaying the isjoin isouter: "+join.isOuter() );
//				System.out.println("displaying the isjoin isright: "+join.isRight() );
//				System.out.println("displaying the isjoin issemi: "+join.isSemi() );
//				System.out.println("displaying the isjoin isstraight: "+join.isStraight() );
//				System.out.println("displaying the isjoin iswindow: "+join.isWindowJoin() );
//				System.out.println("displaying the isjoin isglobal: "+join.isGlobal() );
				
				//if there are where clause for a single table, filter that before hand and then give it to join.
				//if there are where clause for joining the tables, then i think it is better to iterate over and then figure out which
				//tuples to join based on the condition after checking each tuple.

				
				//exp = join.getOnExpression();
				

			} //join loop ends
			
			//initially I added an if clause for if(WHERE==null) but realised its not reqd. despite presence/absense of where clause, i need tohave a join operator because JOIN clause is not null
//			Operator root = new JoinOperator(leftChild, rightChild, exp);
			
			//this wont work directly, needto write a function to concatenate the hasindex of two child tables.  
			//write this function in the join operator. 			
			
//			if(WHERE!=null) {
//				System.out.println("Going to wrap selection operator to the join operator");
//				//need to combine leftand right attributeHashJoins to use in where clause. 
//				for(String key: attributeHashIndex_lChild.keySet()) {
//					root.addMapValue(FROM.toString()+"."+key, attributeHashIndex_lChild.get(key));
//				}
//				for(String key: attributeHashIndex_rChild.keySet()) {
//					root.addMapValue(.toString()+"."+key, attributeHashIndex_lChild.get(key));
//				}
//				root = new SelectionOperator(root, WHERE,  );
//			}
			
			
			return leftChild;
			
		}
		return null;
		
		
		
		//bug fixing attemtps
//		System.out.println("select statement is "+ SELECT);
//		System.out.println("Select contains * "+ SELECT.contains("*"));
//		System.out.println("Select contains X "+ SELECT.contains("X"));
//		System.out.println("Select contains Student.D "+ SELECT.toString().contains("Student.D"));		//true for q3
//		System.out.println("Select contains String object s "+ SELECT.toString().contentEquals("[*]"));		 //yessss..working fine
//		System.out.println("Select contains String object s "+ SELECT.toString().contains("[*]"));
		
	}
	
	
	private static List<Expression> conditionsForTwoTables(List<Expression> listExp, String tableOne, String tableTwo){
		
		System.out.println("....inside conditionsForTwoTables function...checking if this is executinng");
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
			
			//for debugging
//			System.out.println( e.getLeftExpression(). toString().toLowerCase().contains(tableOne.toLowerCase())   );
//			System.out.println( e.getRightExpression().toString().toLowerCase().contains(tableTwo.toLowerCase())   );
//			System.out.println( e.getLeftExpression(). toString().toLowerCase().contains(tableTwo.toLowerCase())   );
//			System.out.println( e.getRightExpression().toString().toLowerCase().contains(tableOne.toLowerCase())   );
			

			if(tableOne.contains("join")) {
				System.out.println("yess it contains a join ed string class ....yes common ash2");
				
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
						System.out.println("Reordering clause 1");
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
				System.out.println(".......Found a join condition for the tables: "+tableOne+" and "+tableTwo);
				if(   e.getLeftExpression().toString().toLowerCase().contains(tableOne.toLowerCase())
						&& e.getRightExpression().toString().toLowerCase().contains(tableTwo.toLowerCase())    ) 
				{
					returnClause.add(e);
				}
				else if(  e.getLeftExpression().toString().toLowerCase().contains(tableTwo.toLowerCase())
						&& e.getRightExpression().toString().toLowerCase().contains(tableOne.toLowerCase()) ) {
					//split the clause. reorder it and join it
					
					System.out.println("Reordering clause 2");
					returnClause.add(reorderClause(e));
				}
			}
			
			}
			
			
		}
		
		return returnClause;
	}
	
	private static ComparisonOperator reorderClause(ComparisonOperator e) {
		ComparisonOperator reversedClause = null;
		
		System.out.println("The original vlause received = "+e.toString());
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
		
		System.out.println("Changing the order of the join condition as it was reversed before.. basically providing commutativity");
		System.out.println("The reordered clause is = "+op.toString());
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
			
			//does this work? need to see
			System.out.println("printing leftExpression "+ e.getLeftExpression());
			System.out.println("priting right edpression "+ e.getRightExpression());
			System.out.println("prining tablename "+table);
			System.out.println(e.getRightExpression() instanceof Column );
				
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
						
			) 
			{
				System.out.println("\n-----Single table");
				returnClause.add(e);
					
			}
				//if(e.getLeftExpression().toString().toLowerCase()) {}
				//if lhs contains rhs, then single tbale exp yes
				//if lhs has the table name and rhs is a constant then yes
				//if rhs is a table name and lhs is a constant then yes
		
		}
        return returnClause;
    }
	
	
	
	
	private static List<Expression> splitExpression(Expression expression) {
        List<Expression> expressions = new ArrayList<>();
        
        //if the expression is a logical conjunction and/or then split it recursively
        if (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            
            expressions.addAll(splitExpression(andExpression.getLeftExpression()));
            expressions.addAll(splitExpression(andExpression.getRightExpression()));
        } 
        //the below wont be of much use to this program as its just conjunction but added anyway to test custom queries. 
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

	
	