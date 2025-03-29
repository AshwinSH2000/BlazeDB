package ed.inf.adbs.blazedb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.statement.select.FromItem;

/*
 * This class contains information about tables loaded into the database and its details such as path and attributes
 * 
 */
public class DatabaseCatalog {
	
	//static makes the entire program to have only one instance of databasecCatalog. 
	private static DatabaseCatalog instance;
	private Map<String, TableInfo> catalogHash;
	
	//Private constructor that can only be called by this class
	private DatabaseCatalog() {
		catalogHash = new HashMap<String, TableInfo>();
	}
	
	//The below function to create the instance of databaseCatalog. 
	//It is not included in the constructor as it is best created only when there is need for it. 
	public static DatabaseCatalog getInstance() {
		if(instance==null) {
			instance = new DatabaseCatalog();
		}
		return instance;
	}

	
	/*
	 * This method visits the path given during the input and 
	 * loads the schema, all the tables and its attributes into the database catalog. 
	 * 
	 * @param path This is the main path given in command line input which consists of schema and all the tables
	 */
	public void loadDetails(String path) {
		
		//path will have "samples/db" because that was passed from the main/initial execution
		String schemaFilePath = path + "/schema.txt";
        try (BufferedReader br = new BufferedReader(new FileReader(schemaFilePath))) {
        	String line;
        	while((line=br.readLine())!=null) {
        		
        		//Splitting by " " as it is mentioned in schema.txt. Hence the first element in the list will be tableName
        		String[] cols = line.split(" ");
        		String tableName = cols[0];
        		
        		List<String> tableAttributes = new ArrayList<>();
        		int index=1;
        		while(index < cols.length) {
        			//Adding tableName.columnName into the catalog
        			tableAttributes.add(tableName.toLowerCase()+"."+cols[index].toLowerCase());
        			index++;
        		}
        		
        		//Hard coded as it was mentioned that all the data will be present in the /data directory inside samples/db. 
        		String tablePath = path + "/data/" + tableName + ".csv";
        		
        		//All three parameters are read. Now time to load the table metadata into the databaseCatalog
        		catalogHash.put(tableName.toLowerCase(), new TableInfo(tableName,tableAttributes,tablePath));
        	}
        	
        } catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	//Function to check the values of the hash map. This is only for debugging
	public void displayCatalogHash() {
		for(Map.Entry<String, TableInfo> map : catalogHash.entrySet()) {
			System.out.println(map.toString());
		}
	}
	
	/*
	 * @param name The name of the table whose information we want to access
	 */
	public TableInfo getTableInfo(String name) {
		return catalogHash.get(name.toLowerCase());
	}
	
	/*
	 * @param name The name of the table whose schema we want to access
	 */
	public List<String> getTableSchema(String name){
		if (catalogHash.containsKey(name.toLowerCase())) {
			return catalogHash.get(name.toLowerCase()).attributes;
		}
		else
			return null;
	}
	
	/*
	 * @param name The name of the table whose path we want to access
	 */
	public String getTableFilePath(String name) {
		if(catalogHash.containsKey(name.toLowerCase())) {
			return catalogHash.get(name.toLowerCase()).path;
		}
		else
			return null;
	}
	
	
}
