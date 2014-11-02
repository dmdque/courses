import java.sql.*;
import java.util.Properties;
import java.util.*;

public class A3 {
  public static boolean DEBUG = true;
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
    for(int col = 0; col < matrix[0].length; col++) {
      for(int row = 0; row < matrix.length; row++) {
        if(matrix[col][row] != 0)
          meList.add(new MatrixEntry(row, col, matrix[col][row]));
      }
    }
    if(DEBUG) {
      for(MatrixEntry me : meList) {
        System.out.println(me);
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
    ArrayList<MatrixEntry> meList = getSparseMatrixFromDB(matrix_id);

    // note: this is not good for space complexity
    double[][] matrix = convertSparseToMatrix(md, meList);
    for(int i = 0; i < md.col; i++) {
      for(int j = 0; j < md.row; j++) {
        System.out.print(matrix[j][i] + "\t"); // note that i and j are reversed for the sake of printing
      }
      System.out.println();
    }
  }

  public static void writeMatrixDB(int matrix_id, double[][] matrix) throws SQLException {
    ArrayList<MatrixEntry> meList = new ArrayList<MatrixEntry>();
    meList = convertMatrixToSparse(matrix);

    for(MatrixEntry me : meList) {
      // write to db
      System.out.println(me);
      setV(matrix_id, me.row, me.col, me.val);
    }
  }

  public static void addMatricies(int id1, int id2) throws SQLException {
    MatrixDimension md1 = checkMatrixExists(id1);
    double[][] matrix1 = convertSparseToMatrix(md1, getSparseMatrixFromDB(id1));
    MatrixDimension md2 = checkMatrixExists(id2);
    double[][] matrix2 = convertSparseToMatrix(md2, getSparseMatrixFromDB(id2));

    if(md1.row == md2.row && md1.col == md2.col) {
      double[][] matrix = createZeroedMatrix(md1);
      for(int col = 0; col < md1.col; col++) {
        for(int row = 0; row < md1.row; row++) {
          matrix[col][row] = matrix1[col][row] + matrix2[col][row];
        }
      }
      for(int i = 0; i < md1.col; i++) {
        for(int j = 0; j < md1.row; j++) {
          System.out.print(matrix[j][i] + "\t"); // note that i and j are reversed for the sake of printing
        }
        System.out.println();
      }
      // TODO: store result back in matrix1
      // delete old matrix1 and store new matrix1
      writeMatrixDB(id1, matrix);
    } else {
      System.out.println("Error: mismatched dimensions");
    }
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

    //query = "SELECT COUNT(*) FROM MATRIX WHERE MATRIX_ID = " + matrix_id;
    //stmt = con.createStatement();
    //rs = stmt.executeQuery(query);
    //while(rs.next()) {
      //if(Integer.parseInt(rs.getString(1)) > 0) {
        //System.out.println("num: " + rs.getString(1));
        //return true;
      //}
    //}
    //return null;
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
        return null;
      } else {
        //System.out.println("ERROR: out of bounds");
        return null;
      }
    } else {
      //System.out.println("ERROR: matrix doesn't exist");
      return null;
    }
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
          System.out.println("query: " + query);
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
      System.out.println("query: " + query);
      Statement stmt = con.createStatement();
      stmt.executeUpdate(query);
      //while (rs.next()) {
      //System.out.println(rs.getString(1));
      //}
    } else {
      System.out.println(md);
      System.out.println("RESIZE NOT YET IMPLEMENTED");
      // TODO: resize matrix
    }
    
  }

  public static void getVWrapper(int matrix_id, int row_dim, int column_dim) throws SQLException {
    System.out.print("GETV: ");
    Double result = getV(matrix_id, row_dim, column_dim);
    if(result == null)
      System.out.println("ERROR");
    else
      System.out.println(result);
  }

  public static void setVWrapper(int matrix_id, int row_dim, int column_dim, double value) throws SQLException {
    System.out.print("SETV: ");
    int result = setV(matrix_id, row_dim, column_dim, value);
    if(result == 0)
      System.out.println("DONE");
    else
      System.out.println("ERROR");
  }

  public static void main(String[] args) throws ClassNotFoundException,SQLException {
    final String CONNECTION_STRING = args[0];
    // Try to connect
    con = DriverManager.getConnection(CONNECTION_STRING);
    System.out.println("Connection Established");

    System.out.println("Before:");
    printSparseMatrix(1);

    setM(1, 5, 5);
    getVWrapper(1, 3, 4);
    
    //setVWrapper(1, 3, 4, 2.3);
    //setVWrapper(1, 3, 2, 2.3);
    System.out.println("State 1:");
    printSparseMatrix(1);
    addMatricies(1, 1);
    System.out.println("State 2:");
    printSparseMatrix(1);

    //String query = "SELECT COUNT(*) FROM LINEITEM AS CNT";
    //Statement stmt = con.createStatement();
    //ResultSet rs = stmt.executeQuery(query);
    //while (rs.next()) {
      //System.out.println(rs.getString(1));
    //}

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
