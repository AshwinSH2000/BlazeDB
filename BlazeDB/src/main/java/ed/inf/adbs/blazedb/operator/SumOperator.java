package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.SelectItem;


public class SumOperator extends Operator{
	
	private Operator root;
	private ExpressionList groupByClause;
	private List<SelectItem<?>> selectClause;
	private Map<String, Integer> attributeHashIndex;
	private Map<String, Integer> projectedAttributeHashIndex;
	private int index;
	private List<Tuple> bufferTuples;
	private List<Tuple> outputTuples;
	
	
	public SumOperator(Operator root, ExpressionList groupByClause, List<SelectItem<?>> selectClause, Map<String, Integer> attributesHashIndex) {
		this.root = root;
		this.groupByClause = groupByClause;
		this.selectClause = selectClause;
		this.attributeHashIndex = attributesHashIndex;
		this.index = 0;
		this.bufferTuples = new ArrayList<>();
		this.outputTuples = new ArrayList<>();
		this.projectedAttributeHashIndex = new HashMap<>();
		
		
		groupTheTuples();
	}
	
	private void groupTheTuples() {
		Tuple tuple;
		while(     (tuple=root.getNextTuple())!=null    ) {
			bufferTuples.add(tuple);
		}
		
		if(selectClause.toString().toLowerCase().contains("sum") && groupByClause == null) {
			
			Tuple tupleToReturn = new Tuple();
			//need to set just a tuple
			//since group by clause is null, i can assume that there is no other selectitem apart from sum(s)
			int sum=0, evalSumExpr=1, count=0;
			
			for(SelectItem<?> sumItem : selectClause) {
				evalSumExpr=1;
				if(sumItem instanceof SelectItem) {										//this was SelectExpressionItem ..next row too
					Expression exp = ((SelectItem)sumItem).getExpression();
					if (exp instanceof Function) {
                        Function function = (Function) exp;
                        
                        Expression parameters = function.getParameters();
                        System.out.println("SUMOP: printiing outside the loop "+parameters.toString());
                        
                        if(!parameters.toString().contains("*")) {
                        	//doesnt contain * that means its either sum(number) or sum(column)
                        	
                        	//if it contains column
                        	if(attributeHashIndex.containsKey(parameters.toString().toLowerCase())) {
                        		for( Tuple tempTuple : bufferTuples) {
                        			sum += tempTuple.get(attributeHashIndex.get(parameters.toString().toLowerCase()));
                        		}
                        	}
                        	
                        	//if it contains a single number inside brackets
                        	else {
                        		evalSumExpr = Integer.parseInt(parameters.toString());
                                sum = bufferTuples.size()*evalSumExpr;

                        	}
                        	
                        }
                        else {
                        	//in this case we check for either sum(number * number [* number ...]) or sum(column * column [* column...])
                        	int ans=1, number=0;
                        	sum=0;
                        	evalSumExpr=1;
                        	String[] numbers = parameters.toString().split("\\*");
                        	
                        	for ( Tuple currentTuple : bufferTuples) {
                        		ans=1;
                        		for(String individualNums : numbers) {
                            		individualNums = individualNums.strip();
                            		
                            		//if it is a column reference
                            		if(attributeHashIndex.containsKey(individualNums.toLowerCase())) {
                            			number = currentTuple.get(attributeHashIndex.get(individualNums.toLowerCase()));
                            		}
                            		else {
                            			number = Integer.parseInt(individualNums);
                            		}
                            		ans = ans * number;
                            	}
                        		sum = sum + ans;
                        	}
                        		
                        }
                        System.out.println("SUMOP: printing the result = "+sum);
                        
                        //calculate the sum based on the number of tuples in bufferTuple
					}
					
				}
				//illi tuple ge add maadbodu
				tupleToReturn.add(sum);
				projectedAttributeHashIndex.put(sumItem.toString().toLowerCase(), count++);
			}
			outputTuples.add(tupleToReturn);
			System.out.println("SUMOP: the output of the expression is "+outputTuples.toString());
			
		}
		
		
		if( !selectClause.toString().toLowerCase().contains("sum") && groupByClause!=null ) {
			//need to just group by. here need to check if the condition in select clause matches the condition in group by clause.
			//the clause present in select needs to be present in group by too... assuming this and proceeding. 
			HashSet<Tuple> uniqueTuples = new HashSet<Tuple>();
			
			for (Tuple scannedTuple : bufferTuples) {
				Tuple tempTuple = new Tuple();
				for(Object groupByObj : groupByClause ) {
					tempTuple.add(   scannedTuple.get(   attributeHashIndex.get(   groupByObj.toString().toLowerCase()  )    )    );
				}
				uniqueTuples.add(tempTuple);
			}
			
			int count=0;
			for(Object groupByObj : groupByClause ) {
				projectedAttributeHashIndex.put(groupByObj.toString().toLowerCase(), count++);
			}
			
			//copy uniqueTuples to outputTuples
			for (Tuple temp : uniqueTuples) {
				outputTuples.add(temp);
			}
		}
		
		if(selectClause.toString().toLowerCase().contains("sum") && groupByClause!=null) {
		;
			//mix of both. first need to group by and then apply the sum. 
		}
	
		
	}
	
	@Override
	public Tuple getNextTuple() {
		
		if (index < outputTuples.size()) {
            return outputTuples.get(index++);
        }
        return null; //no more tuples to return..hence returning null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Map<String, Integer> getAttributeHashIndex() {
		// TODO Auto-generated method stub
		return this.projectedAttributeHashIndex;
	}

	@Override
	protected String getTableName() {
		// TODO Auto-generated method stub
		return null;
	}

}
