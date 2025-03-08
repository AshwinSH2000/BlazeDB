package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOperator extends Operator {

	
	private Operator root;
	private List<SelectItem<?>> SELECT;
	private Map<String, Integer> attributeHashIndex;
	private List<String> colsToBeProjected;

	
	public ProjectionOperator(Operator root, List<SelectItem<?>> SELECT, Map<String, Integer> attributeHashIndex) {
		this.root = root;
		this.SELECT=SELECT;
		this.attributeHashIndex = attributeHashIndex;
		
	    colsToBeProjected = new ArrayList<>();
	    
		for(SelectItem<?> col : this.SELECT) {
			
			if(col instanceof SelectItem) {										//this was SelectExpressionItem ..next row too
				Expression exp = ((SelectItem)col).getExpression();
				
				if(exp instanceof Column) {
					Column column = (Column)exp;
					String tableName = column.getTable().getName();   //column.getTable() != null ? column.getTable().getName() : null;
					String attributeNames = column.getColumnName();
					colsToBeProjected.add(attributeNames);

					
					//just a debug statement to check if the tablename and col name is getting split or not. 
		            //System.out.println("table is " + tableName + " and col is " + attributeNames);

				}
			}
			//System.out.println("cols to be projected = "+colsToBeProjected);
		} //end of for each loop
		
		//System.out.println("cols to be projected = "+colsToBeProjected);

	}
	
	public Tuple ProjectCol(Tuple tuple) {
		
		Tuple projectedTuple = new Tuple();
		
		for(String col : colsToBeProjected) {
			if(attributeHashIndex.containsKey(col)) {
				projectedTuple.add(tuple.get(attributeHashIndex.get(col)));
			}
		}	
		
//		idu beda mostly because attributeHashIndex doesnt consist the hash of tablename and column 	
		
//		DatabaseCatalog dbc = DatabaseCatalog.getInstance();
//		dbc.getTableSchema(null)
		return projectedTuple;
	}
	
	@Override
	public Tuple getNextTuple() {
		
		//not sure if while is needed here. but lets add it for now
		while(true) {
			Tuple tuple = root.getNextTuple();
			if(tuple==null) {
				return null;
			}
			return ProjectCol(tuple); 
		}
	}

	@Override
	public void reset() {
		root.reset();
		//again not sure of this. need to check it. 
	}

	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
