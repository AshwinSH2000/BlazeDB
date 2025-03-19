package ed.inf.adbs.blazedb;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.sql.rowset.serial.SQLOutputImpl;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import ed.inf.adbs.blazedb.operator.Operator;
import ed.inf.adbs.blazedb.operator.QueryPlan;
import ed.inf.adbs.blazedb.operator.ScanOperator;

/**
 * Lightweight in-memory database system.
 *
 * Feel free to modify/move the provided functions. However, you must keep
 * the existing command-line interface, which consists of three arguments.
 *
 */
public class BlazeDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: BlazeDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		// Just for demonstration, replace this function call with your logic

		//List<String> schema = Arrays.asList("A","B", "C", "D");
		//		for( String s : schema) {
		//			System.out.println(s);
		//		}
		//		
		//String tableName = "Student";

		//ScanOperator scan = new ScanOperator(tableName, databaseDir, schema);
		//ScanOperator scan = new ScanOperator(tableName);

//		int i=0;
//		Tuple tuple;
//		while ((tuple = scan.getNextTuple()) != null) {
//
//			;
//		}
//		scan.reset();
//
//		scan.close();

		DatabaseCatalog dbc = DatabaseCatalog.getInstance();
		dbc.loadDetails(databaseDir);
		
		
		parseQuery(inputFile, outputFile);

		//		String x=dbc.getTableFilePath(tableName);
		//		TableInfo y=dbc.getTableInfo(tableName);
		//		List<String> z=dbc.getTableSchema(tableName);
		//		dbc.displayCatalogHash();
		//		System.out.println(x);
		//		System.out.println(y);
		//		System.out.println(z);

	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement
	 * from a file or a string and prints the SELECT and WHERE clauses to screen.
	 */

	public static void parseQuery(String filename, String outputFile) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
			//            Statement statement = CCJSqlParserUtil.parse("SELECT Course.cid, Student.name FROM Course, Student WHERE Student.sid = 3");
			if (statement != null) {

				//create the necessary variables to hold the parsed and broken down SQL commands
				List<SelectItem<?>> SELECT;
				Distinct DISTINCT;	
				List<OrderByElement> ORDERBY;
				GroupByElement GROUPBY;
				Expression WHERE;
				List<Join> JOIN;
				FromItem FROM;
				Select select = (Select) statement;

				SELECT=select.getPlainSelect().getSelectItems();
				FROM=select.getPlainSelect().getFromItem();
				JOIN=select.getPlainSelect().getJoins();
				WHERE=select.getPlainSelect().getWhere();
				GROUPBY=select.getPlainSelect().getGroupBy();
				ORDERBY=select.getPlainSelect().getOrderByElements();
				DISTINCT=select.getPlainSelect().getDistinct();

				System.out.println("Statement: " + select);
				System.out.println("SELECT : " + SELECT);
				System.out.println("DISTINCT : "+ DISTINCT);
				System.out.println("WHERE : " + WHERE);
				System.out.println("GROUP BY : " + GROUPBY);
				System.out.println("ORDER BY : "+ ORDERBY);
				System.out.println("FROM : "+ FROM);				 //but this is displaying only one table. rest in getJoin
				System.out.println("JOIN  : "+JOIN);
				

				
//				Code to test the initialization of databasecatalog				
//				DatabaseCatalog dbc = DatabaseCatalog.getInstance();
//				String x=dbc.getTableFilePath(FROM.toString());
//				TableInfo y=dbc.getTableInfo(FROM.toString());
//				List<String> z=dbc.getTableSchema(FROM.toString());
//				dbc.displayCatalogHash();
//				System.out.println("expecting path= "+x);
//				System.out.println(y);
//				System.out.println(z);
				
				QueryPlan qp = new QueryPlan();
				Operator root = qp.buildQueryPlan(SELECT, DISTINCT, ORDERBY, GROUPBY, WHERE, JOIN, FROM);
				execute(root,outputFile);


			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

	/**
	 * Executes the provided query plan by repeatedly calling `getNextTuple()`
	 * on the root object of the operator tree. Writes the result to `outputFile`.
	 *
	 * @param root The root operator of the operator tree (assumed to be non-null).
	 * @param outputFile The name of the file where the result will be written.
	 */
	public static void execute(Operator root, String outputFile) {
		try {
			// Create a BufferedWriter
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

			// Iterate over the tuples produced by root
			Tuple tuple = root.getNextTuple();
			while (tuple != null) {
				writer.write(tuple.toString());
				writer.newLine();
				System.out.println("hi"+tuple.toString());
				tuple = root.getNextTuple();
			}

			// Close the writer
			writer.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
