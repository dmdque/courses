/*
 * getV
 * setV
 * setM needs modifications
 * delete
 * delete all
 * add
 sub
 mult
 transpose
 sql
*/
import java.sql.*;
import java.util.Properties;
import java.util.*;
import java.io.*;

public class A3 {
  public static boolean DEBUG = false;
  public static boolean DEBUG_SQL = false;
  // The JDBC Connector Class.

  private static final String dbClassName = "com.mysql.jdbc.Driver";
  // Connection string. cs348 is the database the program is trying to connection
  // is connecting to,127.0.0.1 is the local loopback IP address for this machine, user name for the connection 
  // is root, password is cs348
  //private static final String CONNECTION_STRING;
    //"jdbc:mysql://127.0.0.1/TPC-H?user=root&password=Marker4114&Database=tpch;";

  private static Connection con;

  public static double[][] createZeroedMatrix(MatrixDimension md) {
    double[][] matrix = new double[md.col][md.row];
    for(double[] column : matrix)
      Arrays.fill(column, 0);
    return matrix;
  }

  public static ArrayList<MatrixEntry> convertMatrixToSparse(double[][] matrix) {
    ArrayList<MatrixEntry> meList = new ArrayList<MatrixEntry>();
    for(int col = 0; col < matrix.length; col++) {
      for(int row = 0; row < matrix[0].length; row++) {
        if(matrix[col][row] != 0)
          meList.add(new MatrixEntry(row, col, matrix[col][row]));
      }
    }
    return meList;
  }

  public static double[][] convertSparseToMatrix(MatrixDimension md, ArrayList<MatrixEntry> meList) {
    double[][] matrix = createZeroedMatrix(md);
    for(MatrixEntry me : meList)
      matrix[me.col][me.row] = me.val;
    return matrix;
  }

  public static ArrayList<MatrixEntry> getSparseMatrixFromDB(int matrix_id) throws SQLException {
    String query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + matrix_id; // get all tuples related to matrix_id
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery(query);
    ArrayList<MatrixEntry> meList = new ArrayList<MatrixEntry>();
    while (rs.next()) {
      meList.add(new MatrixEntry(Integer.parseInt(rs.getString(2)), Integer.parseInt(rs.getString(3)), Double.parseDouble(rs.getString(4))));
    }
    return meList;
  }

  public static void printSparseMatrix(int matrix_id) throws SQLException {
    MatrixDimension md = checkMatrixExists(matrix_id); // get matrix dimensions
    if(md == null)
      return;
    ArrayList<MatrixEntry> meList = getSparseMatrixFromDB(matrix_id);

    // note: this is not good for space complexity
    double[][] matrix = convertSparseToMatrix(md, meList);
    for(int i = 0; i < md.row; i++) {
      for(int j = 0; j < md.col; j++) {
        System.out.print(matrix[j][i] + "\t"); // note that i and j are reversed for the sake of printing
      }
      System.out.println();
    }
    System.out.println();
  }

  public static void writeSparseToDB(int matrix_id, ArrayList<MatrixEntry> meList) throws SQLException {
    for(MatrixEntry me : meList) {
      // write to db
      setV(matrix_id, me.row, me.col, me.val);
    }
  }

  public static void writeMatrixDB(int matrix_id, double[][] matrix) throws SQLException {
    ArrayList<MatrixEntry> meList = new ArrayList<MatrixEntry>();
    meList = convertMatrixToSparse(matrix);
    writeSparseToDB(matrix_id, meList);
  }

  public static void printMatrix(double[][] matrix) {
    for(int i = 0; i < matrix.length; i++) {
      for(int j = 0; j < matrix[0].length; j++) {
        System.out.print(matrix[j][i] + "\t"); // note that i and j are reversed for the sake of printing
      }
      System.out.println();
    }
  }

  // code copied from add, with '+' changed to '-'
  public static int subMatricies(int id1, int id2) throws SQLException {
    MatrixDimension md1 = checkMatrixExists(id1);
    MatrixDimension md2 = checkMatrixExists(id2);
    double[][] matrix1 = convertSparseToMatrix(md1, getSparseMatrixFromDB(id1));
    double[][] matrix2 = convertSparseToMatrix(md2, getSparseMatrixFromDB(id2));

    if(md1.row == md2.row && md1.col == md2.col) {
      double[][] matrix = createZeroedMatrix(md1);
      for(int col = 0; col < md1.col; col++) {
        for(int row = 0; row < md1.row; row++) {
          matrix[col][row] = matrix1[col][row] - matrix2[col][row];
        }
      }
      if(DEBUG) { printMatrix(matrix); }
      // delete old matrix1 and store new matrix1
      deleteMatrix(id1);
      setM(id1, md1.row, md1.col);
      writeMatrixDB(id1, matrix);
      return 0;
    } else {
      System.out.println("ERROR: mismatched dimensions");
      return 1;
    }
  }

  public static int addMatricies(int id1, int id2) throws SQLException {
    MatrixDimension md1 = checkMatrixExists(id1);
    MatrixDimension md2 = checkMatrixExists(id2);
    double[][] matrix1 = convertSparseToMatrix(md1, getSparseMatrixFromDB(id1));
    double[][] matrix2 = convertSparseToMatrix(md2, getSparseMatrixFromDB(id2));

    if(md1.row == md2.row && md1.col == md2.col) {
      double[][] matrix = createZeroedMatrix(md1);
      for(int col = 0; col < md1.col; col++) {
        for(int row = 0; row < md1.row; row++) {
          matrix[col][row] = matrix1[col][row] + matrix2[col][row];
        }
      }
      if(DEBUG) { printMatrix(matrix); }
      // delete old matrix1 and store new matrix1
      deleteMatrix(id1);
      setM(id1, md1.row, md1.col);
      writeMatrixDB(id1, matrix);
      return 0;
    } else {
      System.out.println("ERROR: mismatched dimensions");
      return 1;
    }
  }

  public static int multMatricies(int id1, int id2) throws SQLException {
    MatrixDimension md1 = checkMatrixExists(id1);
    MatrixDimension md2 = checkMatrixExists(id2);
    double[][] matrix1 = convertSparseToMatrix(md1, getSparseMatrixFromDB(id1));
    double[][] matrix2 = convertSparseToMatrix(md2, getSparseMatrixFromDB(id2));

    if(md1.col != md2.row) {
      return 1;
    }

    MatrixDimension mdResult = new MatrixDimension(md1.row, md2.col);
    double[][] matrix = createZeroedMatrix(mdResult);

    // TODO: matrix multiplication

    // delete old matrix1 and store new matrix1
    deleteMatrix(id1);
    setM(id1, md1.row, md1.col);
    writeMatrixDB(id1, matrix);
    return 0;
  }

  // TODO: modify to consume two matrix_ids and store result in matrix_1
  public static int transposeMatrix(int id1, int id2) throws SQLException {
    // load the matrix into memory
    MatrixDimension md1 = checkMatrixExists(id1);
    MatrixDimension md2 = checkMatrixExists(id2);
    if(md1.row != md2.col || md1.col != md2.row) {
      return 1;
    }
    ArrayList<MatrixEntry> meList = getSparseMatrixFromDB(id2);

    // transpose (for each entry, swap row and col)
    for(MatrixEntry me : meList) {
      int temp = me.col;
      me.col = me.row;
      me.row = temp;
    }
    deleteMatrix(id1); // delete matrix from db
    setM(id1, md2.col, md2.row); // setM with swapped dimensions
    writeSparseToDB(id1, meList); // write to db
    return 0;
  }

  // returns dimensions of the last matrix found, or null if none exist
  public static MatrixDimension checkMatrixExists(int matrix_id) throws SQLException {
    String query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + matrix_id;
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery(query);
    //System.out.println("matrix exists");

    MatrixDimension md = null;
    while (rs.next()) {
      md = new MatrixDimension(Integer.parseInt(rs.getString(2)), Integer.parseInt(rs.getString(3)));
      //System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
    }
    return md;

  }

  public static Double getV(int matrix_id, int row_num, int column_num) throws SQLException {
    // check if matrix exists
    MatrixDimension md = checkMatrixExists(matrix_id);
    if(md != null) {
      if(row_num < md.row && column_num < md.col && row_num >= 0 && column_num >= 0) {
        String query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + matrix_id + " AND ROW_NUM = " + row_num + " AND COL_NUM = " + column_num;
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        MatrixDimension md2 = null;
        while(rs.next()) {
          //System.out.print("value: ");
          //System.out.println(rs.getString("VALUE"));
          return Double.parseDouble(rs.getString("VALUE"));
        }
        //System.out.println("ERROR: entry doesn't exist");
        return 0.0;
      } else {
        //System.out.println("ERROR: out of bounds");
        return null;
      }
    } else {
      //System.out.println("ERROR: matrix doesn't exist");
      return null;
    }
  }


  public static void deleteAll() throws SQLException {
    String query = "DELETE FROM MATRIX_DATA";
    if(DEBUG_SQL) { System.out.println("query: " + query); }
    Statement stmt = con.createStatement();
    stmt.executeUpdate(query);

    query = "DELETE FROM MATRIX";
    if(DEBUG_SQL) { System.out.println("query: " + query); }
    stmt = con.createStatement();
    stmt.executeUpdate(query);
  }

  public static void deleteMatrix(int matrix_id) throws SQLException {
    String query = "DELETE FROM MATRIX_DATA WHERE MATRIX_ID = " + matrix_id;
    if(DEBUG_SQL) { System.out.println("query: " + query); }
    Statement stmt = con.createStatement();
    stmt.executeUpdate(query);

    query = "DELETE FROM MATRIX WHERE MATRIX_ID = " + matrix_id;
    if(DEBUG_SQL) { System.out.println("query: " + query); }
    stmt = con.createStatement();
    stmt.executeUpdate(query);
  }

  // checks if matrix exists
  // checks if in bounds
  public static int setV(int matrix_id, int row_num, int column_num, double value) throws SQLException {
    Double currentValue = getV(matrix_id, row_num, column_num);
    if(currentValue != null) {
      // update the value
      String query = "UPDATE MATRIX_DATA SET VALUE = " + value + " WHERE MATRIX_ID = " + matrix_id + " AND ROW_NUM = " + row_num + " AND COL_NUM = " + column_num;
      if(DEBUG_SQL) { System.out.println("query: " + query); }
      Statement stmt = con.createStatement();
      stmt.executeUpdate(query);
      return 0;
    } else {
      MatrixDimension md = checkMatrixExists(matrix_id);
      if(md != null) {
        if(row_num < md.row && column_num < md.col && row_num >= 0 && column_num >= 0) {
          // INSERT the value
          String query = "INSERT INTO MATRIX_DATA VALUES (" + matrix_id + "," + row_num + "," + column_num + "," + value + ")";
          if(DEBUG_SQL) { System.out.println("query: " + query); }
          Statement stmt = con.createStatement();
          stmt.executeUpdate(query);
          return 0;
        } else {
          System.out.println("ERROR: out of bounds");
          return 1;
        }
      } else {
        //System.out.println("ERROR: matrix doesn't exist");
        return 2;
      }
    }
  }
  
  // checks if matrix already exists
  // if it doesn't creates zeroed matrix
  // otherwise, TODO
  public static void setM(int matrix_id, int row_dim, int column_dim) throws SQLException {
    // create zeroed matrix
    MatrixDimension md = checkMatrixExists(matrix_id);
    if(md == null) {
      String query = "INSERT INTO MATRIX VALUES (" + matrix_id + "," + row_dim + "," + column_dim + ")";
      if(DEBUG_SQL) { System.out.println("query: " + query); }
      Statement stmt = con.createStatement();
      stmt.executeUpdate(query);
      //while (rs.next()) {
      //System.out.println(rs.getString(1));
      //}
    } else {
      System.out.println("RESIZE NOT YET IMPLEMENTED");
      // TODO: resize matrix
    }
    
  }

  public static void transposeMatrixWrapper(int id1, int id2) throws SQLException {
    if(transposeMatrix(id1, id2) == 0)
      System.out.println("DONE");
    else
      System.out.println("ERROR");
  }

  public static void addMatriciesWrapper(int id1, int id2) throws SQLException {
    if(addMatricies(id1, id2) == 0)
      System.out.println("DONE");
    else
      System.out.println("ERROR");
  }

  public static void subMatriciesWrapper(int id1, int id2) throws SQLException {
    if(subMatricies(id1, id2) == 0)
      System.out.println("DONE");
    else
      System.out.println("ERROR");
  }

  public static void deleteAllWrapper() throws SQLException {
    deleteAll();
    System.out.println("DONE");
  }

  public static void deleteMatrixWrapper(int matrix_id) throws SQLException {
    deleteMatrix(matrix_id);
    System.out.println("DONE");
  }

  public static void getVWrapper(int matrix_id, int row_dim, int column_dim) throws SQLException {
    if(DEBUG) { System.out.print("GETV: "); }
    Double result = getV(matrix_id, row_dim, column_dim);
    if(result == null)
      System.out.println("ERROR");
    else
      System.out.println(result);
  }

  public static void setVWrapper(int matrix_id, int row_dim, int column_dim, double value) throws SQLException {
    if(DEBUG) { System.out.print("SETV: "); }
    int result = setV(matrix_id, row_dim, column_dim, value);
    if(result == 0)
      System.out.println("DONE");
    else
      System.out.println("ERROR");
  }

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
    final String CONNECTION_STRING = args[0];
    // Try to connect
    con = DriverManager.getConnection(CONNECTION_STRING);
    System.out.println("Connection Established");


    BufferedReader in = new BufferedReader(new FileReader(args[1]));
    String line = in.readLine();
    while(line != null) {
      System.out.println(line);
      String[] tokens = line.split(" ");
      if(tokens[0].equalsIgnoreCase("SETM")) {
        int id1 = Integer.parseInt(tokens[1]);
        int id2 = Integer.parseInt(tokens[2]);
        int id3 = Integer.parseInt(tokens[3]);
        setM(id1, id2, id3);
      } else if(tokens[0].equalsIgnoreCase("SETV")) {
        int id1 = Integer.parseInt(tokens[1]);
        int id2 = Integer.parseInt(tokens[2]);
        int id3 = Integer.parseInt(tokens[3]);
        double value = Double.parseDouble(tokens[4]);
        setV(id1, id2, id3, value);
      } else if(tokens[0].equalsIgnoreCase("GETV")) {
        int id1 = Integer.parseInt(tokens[1]);
        int id2 = Integer.parseInt(tokens[2]);
        int id3 = Integer.parseInt(tokens[3]);
        getVWrapper(id1, id2, id3);
      } else if(tokens[0].equalsIgnoreCase("ADD")) {
      } else if(tokens[0].equalsIgnoreCase("SUB")) {
      } else if(tokens[0].equalsIgnoreCase("TRANSPOSE")) {
      } else if(tokens[0].equalsIgnoreCase("DELETE")) {
        if(tokens[0].equalsIgnoreCase("ALL")) {
        } else {
        }
      } else if(tokens[0].equalsIgnoreCase("PRINT")) {
        int id1 = Integer.parseInt(tokens[1]);
        printSparseMatrix(id1);
      } else if(tokens[0].equalsIgnoreCase("SETM")) {

      }
      
      line = in.readLine();
    }
    in.close();

    System.out.println("Before:");
    printSparseMatrix(1);

    //setM(1, 3, 5);
    //setM(2, 3, 5);
    //setM(3, 5, 3);
    //setVWrapper(1, 3, 4, 2.3);
    //setVWrapper(1, 3, 2, 2.3);
    //setVWrapper(3, 3, 2, 5);

    //setVWrapper(2, 3, 2, -2.3);
    //getVWrapper(1, 3, 4);
    
    //System.out.println("State 1:");
    //printSparseMatrix(1);
    //printSparseMatrix(2);
    //printSparseMatrix(3);

    //subMatricies(1, 2);
    //System.out.println("State 2:");
    //printSparseMatrix(1);
    //transposeMatrixWrapper(1, 2);
    //transposeMatrixWrapper(1, 3);
    //System.out.println("after transpose: ");
    //printSparseMatrix(1);

    //deleteMatrixWrapper(1);
    //printSparseMatrix(1);
    //deleteAllWrapper();
    
    con.close();
  }

}

class MatrixEntry implements Comparable<MatrixEntry> {
  public int row;
  public int col;
  public double val;

  public MatrixEntry(int row, int col, double val) {
    this.row = row;
    this.col = col;
    this.val = val;
  }

  public String toString() {
    return "row: " + this.row + " col: " + this.col + " val: " + this.val;
  }

  public int compareTo(MatrixEntry me) {
    if(this.row == me.row) {
      return this.col - me.col;
    } else {
      return this.row - me.row;
    }
  }
}
class MatrixDimension {
  public int row;
  public int col;

  public MatrixDimension(int row, int col) {
    this.row = row;
    this.col = col;
  }

  public String toString() {
    return "row: " + this.row + " col: " + this.col;
  }

}
