package ed.inf.adbs.blazedb.operator;

import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;

public class ProjectionOperator extends Operator {

	
	Operator root;
	List<String> SELECT;
	
	public ProjectionOperator(Operator root, List<String> SELECT) {
		this.root = root;
		this.SELECT=SELECT;
	}
	
	public Tuple ProjectCol(Tuple tuple) {
		
		DatabaseCatalog dbc = DatabaseCatalog.getInstance();
//		dbc.getTableSchema(null)
		return tuple;
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
