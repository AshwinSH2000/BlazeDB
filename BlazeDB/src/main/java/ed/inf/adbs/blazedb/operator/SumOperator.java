package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.serial.SQLOutputImpl;

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
		
		System.out.println("INSIDE SUM OPERATOR....1");
		
		//if there are no tuples obtained from the child operator, return immediately and avoid all unnecessary computations below
		if(bufferTuples.size()==0){
			return;
		}
		
		if(selectClause.toString().toLowerCase().contains("sum") && groupByClause == null) {
			
			System.out.println("INSIDE SUM OPERATOR....2");
			
			Tuple tupleToReturn = new Tuple();
			//need to set just a tuple
			//since group by clause is null, i can assume that there is no other selectitem apart from sum(s)
			int sum=0, evalSumExpr=1, count=0;
			
			for(SelectItem<?> sumItem : selectClause) {
				evalSumExpr=1;
				sum=0;
				if(sumItem instanceof SelectItem) {										//this was SelectExpressionItem ..next row too
					Expression exp = ((SelectItem)sumItem).getExpression();
					if (exp instanceof Function && exp.toString().toLowerCase().contains("sum")) {
                        Function function = (Function) exp;
                        System.out.println("SUMOP: printiing the function "+exp.toString());

                        Expression parameters = function.getParameters();
                        System.out.println("SUMOP: printiing outside the loop "+parameters.toString());
                        
                        if(!parameters.toString().contains("*")) {
                        	//doesnt contain * that means its either sum(number) or sum(column)
                        	
                        	//if it contains column
                        	if(attributeHashIndex.containsKey(parameters.toString().toLowerCase())) {
                        		for( Tuple tempTuple : bufferTuples) {
                        			sum += tempTuple.get(attributeHashIndex.get(parameters.toString().toLowerCase()));
                            		System.out.println("DEBUGGING....1: sum = "+sum);

                        		}
                        	}
                        	
                        	//if it contains a single number inside brackets
                        	else {
                        		evalSumExpr = Integer.parseInt(parameters.toString());
                                sum = bufferTuples.size()*evalSumExpr;
                        		System.out.println("DEBUGGING....2: sum = "+sum);

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
                            		System.out.println("DEBUGGING....3: answer = "+ans);
                            	}
                        		sum = sum + ans;
                        		System.out.println("DEBUGGING....4: sum = "+sum);

                        	}
                        		
                        }
                        System.out.println("SUMOP: printing the result = "+sum);
                        //calculate the sum based on the number of tuples in bufferTuple
					}
					else
					{
						return;
					}
					
				}
				//illi tuple ge add maadbodu
				tupleToReturn.add(sum);
				projectedAttributeHashIndex.put(sumItem.toString().toLowerCase(), count++);
				System.out.println(projectedAttributeHashIndex.toString()+" is the 39845793 93847593 9847593 9384753 939 39847");
			}
			outputTuples.add(tupleToReturn);
			System.out.println("SUMOP: the output of the expression is "+outputTuples.toString());
			
		}
		
		
		if( !selectClause.toString().toLowerCase().contains("sum") && groupByClause!=null ) {
			
			System.out.println("INSIDE SUM OPERATOR....3");
			
			//need to just group by. here need to check if the condition in select clause matches the condition in group by clause.
			//the clause present in select needs to be present in group by too... assuming this and proceeding. 
			HashSet<Tuple> uniqueTuples = new HashSet<Tuple>();
			
			for (Tuple scannedTuple : bufferTuples) {
				Tuple tempTuple = new Tuple();
				for(Object groupByObj : groupByClause ) {
					//bug is here
					
					if(selectClause.toString().toLowerCase().contains(groupByObj.toString().toLowerCase())) {
						System.out.println("SumOperator x: The value of group by obj is here: "+groupByObj.toString() );
						tempTuple.add(   scannedTuple.get(   attributeHashIndex.get(   groupByObj.toString().toLowerCase()  )    )    );
					}
					
				}
				uniqueTuples.add(tempTuple);
			}
			
			int count=0;
			for(Object groupByObj : groupByClause ) {
				if(selectClause.toString().toLowerCase().contains(groupByObj.toString().toLowerCase())) {

				projectedAttributeHashIndex.put(groupByObj.toString().toLowerCase(), count++);
				}
			}
			
			System.out.println("This is the aHI after putting the necessary typles: "+ projectedAttributeHashIndex.toString());
			
			//copy uniqueTuples to outputTuples
			for (Tuple temp : uniqueTuples) {
				outputTuples.add(temp);
			}
		}
		
		if(selectClause.toString().toLowerCase().contains("sum") && groupByClause!=null) {
			
			System.out.println("INSIDE SUM OPERATOR....4");
			HashMap<Tuple, Tuple> uniqueTuples = new HashMap<Tuple, Tuple>();

		
			//mix of both. first need to group by and then apply the sum. 
			for (Tuple scannedTuple: bufferTuples) {
				Tuple tupleHashKey = new Tuple();
				
				for(Object obj : groupByClause) {
					tupleHashKey.add(scannedTuple.get(attributeHashIndex.get(obj.toString().toLowerCase())));
					System.out.println("SUM OPERATOR: "+tupleHashKey.toString());
				}
				Tuple tupleHashValue = new Tuple();
				int colValue=0;
				
				
				if(uniqueTuples.containsKey(tupleHashKey))
				{
					Tuple presentTuple = uniqueTuples.get(tupleHashKey);
//					System.out.println("The tuple that was already PRESENT INSIDE UNIQIE TUPLES IS................."+presentTuple.toString());
					int presentValue = 0;
					System.out.println("Its present");
					for (SelectItem<?> item : selectClause) {
						Expression exp = ((SelectItem)item).getExpression();
						if(exp instanceof Function) {
							Function function = (Function) exp;
							
							Expression parameters = function.getParameters();
							String[] stringParameters= parameters.toString().split("\\*");
							if(stringParameters.length==1) {
		                    	
		                    	try {
		                    		colValue = Integer.parseInt(stringParameters[0].strip());
		                    	}catch(Exception e) {
		                    		colValue = scannedTuple.get(attributeHashIndex.get(stringParameters[0].toLowerCase()));
		                    	}
		                    	System.out.println("The COLUMN VALUE OF THIS SINGLE VALUE INSIDE SUM IS "+colValue);
		                    	
		                    	
		                    	presentValue = presentTuple.get(projectedAttributeHashIndex.get(exp.toString().toLowerCase()));
		                    	tupleHashValue.add(presentValue+colValue);
		                    }
							else {
								//it means parameters are more than 1
								int product=1;
								for (String indvParams : stringParameters) {

		                    		try {
		                    			colValue = Integer.parseInt(indvParams.strip());
		                    			product = product * colValue;
		                    		}catch(Exception e) {
		                    			colValue = scannedTuple.get(attributeHashIndex.get(indvParams.strip().toLowerCase()));
		                    			product = product * colValue;
		                    		}
		                    	}
								
		                    	System.out.println("The product of the terms inside the column is "+product);
		                    	presentValue = presentTuple.get(projectedAttributeHashIndex.get(exp.toString().toLowerCase()));
		                    	tupleHashValue.add(product+presentValue);
							}
						}
						else {
							//it is not a function at all.. which means a normal column that must be summed here. 
							colValue=scannedTuple.get(attributeHashIndex.get(exp.toString().toLowerCase()));
							//presentValue = presentTuple.get(projectedAttributeHashIndex.get(exp.toString().toLowerCase()));
							//tupleHashValue.add(colValue+presentValue);
							tupleHashValue.add(colValue);

							
						}
						
					}
					uniqueTuples.put(tupleHashKey, tupleHashValue);
					System.out.println("One of the tuples that i just put in is..." + uniqueTuples.toString());

				}
				else
				{
					System.out.println("its not present");
					int counter=0;
					//iterate over the select clause and select the columns to be put into it
					for (SelectItem<?> item: selectClause) {
						
						//if it is instanceof function, extract uska inside
						//if inside is an integer, put it directly 
						//if it is a col, then extract uska value
						
						Expression exp = ((SelectItem)item).getExpression();

						if(exp instanceof Function) {
							//eg sum(integer) or sum(column)
							Function function = (Function) exp;
							
							//this might give error is functions arent passed no?
							//umm no because i am coming into this if case only after checking if it is a function
							
	                        Expression parameters = function.getParameters();
//	                        System.out.println(parameters.toString());
	                        
	                        String[] stringParameters= parameters.toString().split("\\*");
		                    
		                    if(stringParameters.length==1) {
		                    	
		                    	try {
		                    		colValue = Integer.parseInt(stringParameters[0].strip());
		                    	}catch(Exception e) {
		                    		colValue = scannedTuple.get(attributeHashIndex.get(stringParameters[0].toLowerCase()));
		                    	}
		                    	System.out.println("The COLUMN VALUE OF THIS SINGLE VALUE INSIDE SUM IS "+colValue);
		                    	tupleHashValue.add(colValue);
		                    	
		                    	projectedAttributeHashIndex.putIfAbsent(  exp.toString().toLowerCase(), counter++  );
		                    	
		                    	
		                    	
		                    }
		                    else {
		                    	//it means the parameters are more than one
		                    	//we do not know if it is a column value of integer.. so iterate over all and check in each chase
		                    	int product = 1;
		                    	for (String indvParams : stringParameters) {

		                    		try {
		                    			colValue = Integer.parseInt(indvParams.strip());
		                    			product = product * colValue;
		                    		}catch(Exception e) {
		                    			colValue = scannedTuple.get(attributeHashIndex.get(indvParams.strip().toLowerCase()));
		                    			product = product * colValue;
		                    		}
		                    	}
		                    	System.out.println("The product of the terms inside the column is "+product);
		                    	tupleHashValue.add(product);
		                    	projectedAttributeHashIndex.putIfAbsent(  exp.toString().toLowerCase(), counter++  );


		                    }

	                        
	         	                        
						}
						else
						{
							//since it is not a function it has to be a normal column of the table
							colValue=scannedTuple.get(attributeHashIndex.get(exp.toString().toLowerCase()));
							tupleHashValue.add(colValue);
							
	                    	projectedAttributeHashIndex.putIfAbsent(  exp.toString().toLowerCase(), counter++  );

						}
					}
					uniqueTuples.put(tupleHashKey, tupleHashValue);
					System.out.println("One of the tuples that i just put in is..." + uniqueTuples.toString());
				
				}

			}
			System.out.println("projectedaHI is..." + projectedAttributeHashIndex.toString());

			
			for (Map.Entry<Tuple, Tuple> entry : uniqueTuples.entrySet()) {
				outputTuples.add(entry.getValue());

			}
		}
		
		System.out.println("INSIDE SUM OPERATOR....5");
		
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
