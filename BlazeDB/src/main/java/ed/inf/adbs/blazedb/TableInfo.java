package ed.inf.adbs.blazedb;

import java.util.List;

/*
 * This is a class created to store the details of the tables. 
 * A new object will be created for every table that is read from the schema.
 * Each object will consist of tableName, the path and all the attributes. 
 */
public class TableInfo {
	String tableName;
	List <String> attributes;
	String path;
	
	/*
	 * This is a constructor that loads the object with the data that is read from schema.txt
	 * 
	 * @param tablename The name of the table read in the schema.txt file
	 * @param attributes The list of attributes corresponding to the above table
	 * @param path The file path where the table can be found	
	 */
	public TableInfo(String tablename, List<String> attributes, String path) {
		this.tableName =  tablename;
		this.attributes = attributes;
		this.path = path;
	}
}
