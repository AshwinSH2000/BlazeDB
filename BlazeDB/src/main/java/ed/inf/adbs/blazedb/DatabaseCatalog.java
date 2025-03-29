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

public class DatabaseCatalog {
	
	// private static makes it to have only one instance/copy of databasecCatalog. 
	private static DatabaseCatalog instance;
	private Map<String, TableInfo> catalogHash;
	
	//private constructor that can only be called by this class
	private DatabaseCatalog() {
		catalogHash = new HashMap<String, TableInfo>();
	}
	
	// the function to create the instance of databaseCatalog. 
	// it is created only when this is called and needed. 
	public static DatabaseCatalog getInstance() {
		if(instance==null) {
			instance = new DatabaseCatalog();
		}
		return instance;
	}
	
	/*
	 *  only path given in input = samples/db
	 *  schema path = samples/db/schema.txt 
	 *  tables path = samples/db/data/<name>.csv
	 */
	
	/*
	 * @param path it is the main path given in command line input which consists of schema and all the tables
	 */
	public void loadDetails(String path) {
		
		//path will have samples/db because that was passed from the main/initial execution
		String schemaFilePath = path + "/schema.txt";
        try (BufferedReader br = new BufferedReader(new FileReader(schemaFilePath))) {
        	String line;
        	while((line=br.readLine())!=null) {
        		String[] cols = line.split(" ");
        		String tableName = cols[0];
        		
        		List<String> tableAttributes = new ArrayList<>();
        		int index=1;
        		while(index < cols.length) {
        			//System.out.println("cols[index] is "+cols[index]);
        			tableAttributes.add(tableName.toLowerCase()+"."+cols[index].toLowerCase());
        			index++;
        		}
        		
        		String tablePath = path + "/data/" + tableName + ".csv";
        		
        		//all three parameters are read. now time to load the table metadata into the databaseCatalog
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
	
	//function to check the values of the hash map. this is only for debugging
	public void displayCatalogHash() {
		for(Map.Entry<String, TableInfo> map : catalogHash.entrySet()) {
			
		}
	}
	
	/*
	 * @param name the name of the table whose information we want to access
	 */
	public TableInfo getTableInfo(String name) {
		return catalogHash.get(name.toLowerCase());
	}
	
	/*
	 * @param name the name of the table whose schema we want to access
	 */
	public List<String> getTableSchema(String name){
		if (catalogHash.containsKey(name.toLowerCase())) {
			return catalogHash.get(name.toLowerCase()).attributes;
		}
		else
			return null;
	}
	
	public String getTableFilePath(String name) {
		if(catalogHash.containsKey(name.toLowerCase())) {
			return catalogHash.get(name.toLowerCase()).path;
		}
		else
			return null;
	}
	
	
}
