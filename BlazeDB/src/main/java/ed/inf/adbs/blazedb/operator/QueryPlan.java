package ed.inf.adbs.blazedb.operator;

import java.util.List;

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
	
	public static Operator buildQueryPlan(List<SelectItem<?>> SELECT, Distinct DISTINCT, List<OrderByElement> ORDERBY,
									  GroupByElement GROUPBY, Expression WHERE, List<Join> JOIN, FromItem FROM) {
		
		DatabaseCatalog catalog = DatabaseCatalog.getInstance();
		
		//need to start from the scan op now. 
		Operator root=new ScanOperator(FROM.toString());
		
		//now i am adding the from and where but i need to add the join clause sometimes. 
//		if(WHERE!=null) {
//			root = new SelectionOperator(root, WHERE);
//		}
//		
		
		
		
		
		return root;
	}
}
