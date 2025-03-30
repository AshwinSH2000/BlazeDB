package ed.inf.adbs.blazedb.operator;

import java.io.*;
import java.util.*;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.statement.select.FromItem;

/*
 * This class defines the ScanOperator which is responsible for opening the files containing the tables data and read it row wise. 
 * Also creates a HashMap that is used extensively in many other operators
 */
public class ScanOperator extends Operator {

	private BufferedReader reader;
	private String tableName;
	private String filePath;
	private List<String> schema;
	private String currentLine;
	private Map<String, Integer> attributeHashIndex;
	int noOfTuples;
	boolean isEmpty;

	
	/*
	 * This is the constructor of this class. Apart from initializing the class variables, it also creates a HashMap. 
	 * The HashMap contains a mapping from the columnNames of the table to an integer value representing the column's position in the table
	 * for example, Student.A=0, Student.B=1, Student.C=2 and so on
	 * @param tableName It is the name of the table which must be read row-wise
	 */
	public ScanOperator(String tableName) {       
				
		this.tableName = tableName.toLowerCase();
		this.filePath = DatabaseCatalog.getInstance().getTableFilePath(tableName);       
		this.schema = DatabaseCatalog.getInstance().getTableSchema(tableName);			  
		
		this.attributeHashIndex = new HashMap<>();
		
		//Handling empty table
		if(this.schema==null) {
			isEmpty = true;
			return;
		}
		isEmpty=false;
		int index=0;
		for(String x:  this.schema) {
			attributeHashIndex.put(x, index++);
		}
		
		noOfTuples=0;
		openFile();
	}
	
	/*
	 * Getter method to return the name of the table
	 * @return tableName Contains the name of the table to be returned
	 */
	public String getTableName() {
		return tableName;
	}
	
	
	/*
	 * Getter method to return the schema of the table
	 * @return (list<String>) returns a list of strings that contain schema of the table
	 */
	public List<String> getSchema(){
		return this.schema;
	}

	
	/*
	 * Method that opens the file containing data about the table.
	 * It uses the filePath argument to open the table to read its data
	 */
	private void openFile() {
		try {
			reader = new BufferedReader(new FileReader(filePath));
		} catch (FileNotFoundException e) {
			System.err.println("Error: Table file not found: " + filePath);
			e.printStackTrace();
		}
	}


	/*
	 * Method to read the opened file line by line and store the data in a list of strings. 
	 * It then parses these strings to store the data in a tuple (which is a list of integers)
	 * @return tuple The row of data that is read from the file is returned
	 */
	@Override
	public Tuple getNextTuple() {
		if (isEmpty) {
			//Handling empty tables
			return null;
		}
		try {
			if ((currentLine = reader.readLine()) != null) {
				// Convert the CSV line into a list of integers (since all values are integers)
				String[] values = currentLine.split(",");
				Tuple tuple = new Tuple();
				for (String value : values) {
					tuple.add(Integer.parseInt(value.trim()));
				}
				noOfTuples++;
				return tuple;
			} else {
				return null; // End of file
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	
	/*
	 * Method used to reset the state of the operator and ask it to return tuples from the very beginning. 
	 * Closing and reopening the file sets it to read it from the beginning
	 */
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
	 * getter function to return the attribute hash map containing mapping from attribute to integer positions in table
	 * @return attributeHashIndex HashMap mapping table attributes to their corresponding integer position values
	 */
	public Map<String, Integer> getAttributeHashIndex(){
		return attributeHashIndex;
	}
	
	
	/*
	 * Method that closes the file after is read completely
	 */
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


