package ed.inf.adbs.blazedb;

import java.util.List;

public class TableInfo {
	String TableName;
	List <String> Attributes;
	String Path;
	
	/*
	 * @param tablename the name of the table read in the schema.txt file
	 * @param attributes the list of attributes corresponding to the above table
	 * @param path the file path where the table can be found
	 */
	public TableInfo(String tablename, List<String> attributes, String path) {
		this.TableName =  tablename;
		this.Attributes = attributes;
		this.Path = path;
	}
}
