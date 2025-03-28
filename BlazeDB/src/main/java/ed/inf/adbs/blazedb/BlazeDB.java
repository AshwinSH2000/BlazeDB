package ed.inf.adbs.blazedb;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.sql.rowset.serial.SQLOutputImpl;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
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

		//this if block checks for the number of arguments passed from command line. 
		if (args.length != 3) {
			System.err.println("Usage: BlazeDB database_dir input_file output_file");
			return;
		}
		
		//storing the command line arguments in their respective variables
		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		//creating the database catalog which consists of details about all the tables and schema. 
		DatabaseCatalog dbc = DatabaseCatalog.getInstance();
		
		//loading the database catalog with the contents present in samples/db folder (in this case) or any folder specified during runtime. 
		dbc.loadDetails(databaseDir);
		
		//parsing the input SQL file and writing to the output. 
		parseQuery(inputFile, outputFile);

	}

	/**
	 * JSQLParser method. Reads SQL statement from a string and stores them in separate variables
	 * @param fileName The name of the file that contains the SQL query to be parsed 
	 * @param outputFile The name of the file where the result will be written.
	 */

	public static void parseQuery(String fileName, String outputFile) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(fileName));
			if (statement != null) {

				//create the necessary variables to hold the parsed and broken down SQL commands
				List<SelectItem<?>> SELECT;
				Distinct DISTINCT;	
				List<OrderByElement> ORDERBY;
				ExpressionList GROUPBY;
				Expression WHERE;
				List<Join> JOIN;
				FromItem FROM;
				Select select = (Select) statement;

				//parsing and extracting all the part of the SQL query
				SELECT=select.getPlainSelect().getSelectItems();
				List<SelectItem<?>> x =select.getPlainSelect().getSelectItems();
				FROM=select.getPlainSelect().getFromItem();
				JOIN=select.getPlainSelect().getJoins();
				WHERE=select.getPlainSelect().getWhere();
				ORDERBY=select.getPlainSelect().getOrderByElements();
				DISTINCT=select.getPlainSelect().getDistinct();
				
				//checking for null is essential as getGroupByExpressionList raises exception when getGroupBy returns null
				if(select.getPlainSelect().getGroupBy()==null) 
					GROUPBY = null;
				else
					GROUPBY=select.getPlainSelect().getGroupBy().getGroupByExpressionList();


				//code to display the parsed bits from main SQL query. commenting this for final submission. 
				
//				System.out.println("Statement: " + select);
//				System.out.println("SELECT : " + SELECT);
//				System.out.println("DISTINCT : "+ DISTINCT);
//				System.out.println("WHERE : " + WHERE);
//				System.out.println("GROUP BY : " + GROUPBY);
//				System.out.println("ORDER BY : "+ ORDERBY);
//				System.out.println("FROM : "+ FROM);				 
//				System.out.println("JOIN  : "+JOIN);
				
				//creating a root and passing all the parsed clauses to construct the root operator. 
				Operator root = QueryPlan.buildQueryPlan(SELECT, DISTINCT, ORDERBY, GROUPBY, WHERE, JOIN, FROM);
				executeQuery(root,outputFile);

			}
		} catch (Exception e) {
			//catch block to hold any exception that occurred during the parsing of the input SQL file
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
	public static void executeQuery(Operator root, String outputFile) {
		try {
			// Create a BufferedWriter
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

			// Iterate over the tuples produced by root
			Tuple tuple = root.getNextTuple();
			while (tuple != null) {
				writer.write(tuple.toString());
				writer.newLine();
				System.out.println("final outpur: "+tuple.toString());
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
