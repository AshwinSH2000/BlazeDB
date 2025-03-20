package ed.inf.adbs.blazedb.operator;

import java.io.*;
import java.util.*;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.statement.select.FromItem;

public class ScanOperator extends Operator {

	private BufferedReader reader;
	private String tableName;
	private String filePath;
	private List<String> schema;
	private String currentLine;
	private Map<String, Integer> attributeHashIndex;

	public ScanOperator(String tableName) {       
		
		DatabaseCatalog catalog = DatabaseCatalog.getInstance();
		
		this.tableName = tableName.toLowerCase();
		this.filePath = DatabaseCatalog.getInstance().getTableFilePath(tableName);       
		this.schema = DatabaseCatalog.getInstance().getTableSchema(tableName);			  
		
		this.attributeHashIndex = new HashMap<>();

		int index=0;
		for(String x:  this.schema) {
			//attributeHashIndex.put(tableName+"."+x, index++);
			attributeHashIndex.put(x, index++);
		}
		
		
		System.out.println("SCANOP: The attributeHashIndex for "+tableName+ "table is "+attributeHashIndex.toString());
//		for(String x: this.schema) {
//			System.out.println("Ashwishshshsh--------"+attributeHashIndex.values());
//		}
		openFile();
	}
	
	
	public String getTableName() {
		return tableName;
	}
	
	public List<String> getSchema(){
		return this.schema;
	}

	private void openFile() {
		try {
			reader = new BufferedReader(new FileReader(filePath));
		} catch (FileNotFoundException e) {
			System.err.println("Error: Table file not found: " + filePath);
			e.printStackTrace();
		}
	}


	@Override
	public Tuple getNextTuple() {
		try {
			if ((currentLine = reader.readLine()) != null) {
				// Convert the CSV line into a list of integers (since all values are integers)
				String[] values = currentLine.split(",");
				Tuple tuple = new Tuple();
				System.out.println();
				for (String value : values) {
					//       System.out.println("Ths is in getNextTuple function: " + value );
					//System.out.print(value + "\t");
					tuple.add(Integer.parseInt(value.trim()));
				}
				System.out.println();
				return tuple;
			} else {
				return null; // End of file
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void reset() {
		try {
			reader.close();
			openFile(); 				// Reopen the file to reset the iterator
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * getter function to return the hash index. 
	 * @return - hash index mapping table attributes to their corresponding int values
	 */
	public Map<String, Integer> getAttributeHashIndex(){
		return attributeHashIndex;
	}
	
	public void close() {
		try {
			if (reader != null) {
				reader.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}


