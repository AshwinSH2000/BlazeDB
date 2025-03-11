package ed.inf.adbs.blazedb.operator;

import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

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
			
			Expression exp = null;

			System.out.println(FROM.toString()+" lallalla "+JOIN.toString());
			
			for( Join join : JOIN) {
				//System.out.println("join var = "+join.toString());
				
				//need to extract the join clause. see the optimal methods. 

				//this loop is to iteratively handle all the tables in JOIN but as of not it is just focussing on one table. 
				//maybe create a List of Joins again to have multiple tables on the fly
				
				
				Operator rightChild = new ScanOperator(join.getRightItem().toString());
				Map<String, Integer> attributeHashIndex_rChild = rightChild.getAttributeHashIndex();
				
				if (WHERE != null)// && isConditionForTable(WHERE, join.getRightItem().toString())) {
				{    System.out.println("Applying SelectionOperator for " + join.getRightItem().toString());
	                rightChild = new SelectionOperator(rightChild, WHERE, attributeHashIndex_rChild);
	            }
				
				leftChild = new JoinOperator(leftChild, rightChild);

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
				

			}
			
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
		
		
		
		//bug fixing attemtps
//		System.out.println("select statement is "+ SELECT);
//		System.out.println("Select contains * "+ SELECT.contains("*"));
//		System.out.println("Select contains X "+ SELECT.contains("X"));
//		System.out.println("Select contains Student.D "+ SELECT.toString().contains("Student.D"));		//true for q3
//		System.out.println("Select contains String object s "+ SELECT.toString().contentEquals("[*]"));		 //yessss..working fine
//		System.out.println("Select contains String object s "+ SELECT.toString().contains("[*]"));

		
		return null;
		
	}
//	private static boolean isConditionForTable(Expression whereClause, String tableName) {
//        if (whereClause instanceof BinaryExpression binaryExpr) {
//            Expression left = binaryExpr.getLeftExpression();
//            Expression right = binaryExpr.getRightExpression();
//
//            if (left instanceof Column leftColumn && leftColumn.getTable() != null) {
//                return leftColumn.getTable().getName().equalsIgnoreCase(tableName);
//            }
//            if (right instanceof Column rightColumn && rightColumn.getTable() != null) {
//                return rightColumn.getTable().getName().equalsIgnoreCase(tableName);
//            }
//        }
//        return false;
//    }
}
