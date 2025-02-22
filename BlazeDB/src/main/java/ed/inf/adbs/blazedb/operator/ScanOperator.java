package ed.inf.adbs.blazedb.operator;

import java.io.*;
import java.util.*;

import ed.inf.adbs.blazedb.Tuple;

public class ScanOperator extends Operator {
	
	private BufferedReader reader;
    private String tableName;
    private String filePath;
    private List<String> schema;
    private String currentLine;

    public ScanOperator(String tableName, String dbPath, List<String> schema) {
        this.tableName = tableName;
        this.filePath = dbPath + "/data/" + tableName + ".csv";
        this.schema = schema;
        openFile();
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
                for (String value : values) {
                	System.out.println("Ths is in getNextTuple function: " + value );
                    tuple.add(Integer.parseInt(value.trim()));
                }
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
            openFile(); // Reopen the file to reset the iterator
        } catch (IOException e) {
            e.printStackTrace();
        }
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


