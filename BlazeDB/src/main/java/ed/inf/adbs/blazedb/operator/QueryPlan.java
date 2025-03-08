package ed.inf.adbs.blazedb.operator;

import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import net.sf.jsqlparser.expression.Expression;
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
			Operator rightChild = null ;
			Expression exp = null;

			System.out.println(FROM.toString()+" lallalla "+JOIN.toString());
			for( Join join : JOIN) {

				//this loop is to iteratively handle all the tables in JOIN but as of not it is just focussing on one table. 
				//maybe create a List of Joins again to have multiple tables on the fly
				rightChild = new ScanOperator(join.toString());
				//exp = join.getOnExpression();
				

			}
			
			Operator root = new JoinOperator(leftChild, rightChild);//, exp);
			return root;
			
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
}
