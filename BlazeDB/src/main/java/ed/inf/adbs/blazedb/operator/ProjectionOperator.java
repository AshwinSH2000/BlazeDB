package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.HashMap;
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
	private Map<String, Integer> projectedAttributesHashIndex;

	
	public ProjectionOperator(Operator root, List<SelectItem<?>> SELECT, Map<String, Integer> attributeHashIndex) {
		this.root = root;
		this.SELECT=SELECT;
		this.attributeHashIndex = attributeHashIndex;
		this.projectedAttributesHashIndex = new HashMap<>();
		
	    colsToBeProjected = new ArrayList<>();
	    
	    int index=0;
		for(SelectItem<?> col : this.SELECT) {
			
			if(col instanceof SelectItem) {										//this was SelectExpressionItem ..next row too
				Expression exp = ((SelectItem)col).getExpression();
				
				if(exp instanceof Column) {
					Column column = (Column)exp;
					String tableName = column.getTable().getName();   //column.getTable() != null ? column.getTable().getName() : null;
					String attributeNames = column.getColumnName();
					colsToBeProjected.add(column.toString().toLowerCase());
					projectedAttributesHashIndex.put(column.toString().toLowerCase(), index++);
		
				}
			}
			
		} //end of for each loop

	}
	
	public Tuple ProjectCol(Tuple tuple) {
		
		Tuple projectedTuple = new Tuple();
		
		for(String col : colsToBeProjected) {
			if(attributeHashIndex.containsKey(col)) {
				projectedTuple.add(tuple.get(attributeHashIndex.get(col)));
			}
		}	
		
		return projectedTuple;
	}
	
	@Override
	public Tuple getNextTuple() {
		
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
	}

	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		// TODO Auto-generated method stub

		return projectedAttributesHashIndex;
	}

	@Override
	protected String getTableName() {
		// TODO Auto-generated method stub
		return root.getTableName();
	}
	
}
