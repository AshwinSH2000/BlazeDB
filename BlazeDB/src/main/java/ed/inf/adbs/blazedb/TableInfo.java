package ed.inf.adbs.blazedb;

import java.util.List;

public class TableInfo {
	String TableName;
	List <String> Attributes;
	String Path;
	
	public TableInfo(String tablename, List<String> attributes, String path) {
		this.TableName =  tablename;
		this.Attributes = attributes;
		this.Path = path;
	}
}
