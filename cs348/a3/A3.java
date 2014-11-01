import java.sql.*;
import java.util.Properties;

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
      System.out.println("NOT YET IMPLEMENTED");
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

    setM(1, 5, 5);
    getVWrapper(1, 3, 4);
    
    setVWrapper(1, 3, 4, 2.3);
    setVWrapper(1, 3, 2, 2.3);
    //String query = "SELECT COUNT(*) FROM LINEITEM AS CNT";
    //Statement stmt = con.createStatement();
    //ResultSet rs = stmt.executeQuery(query);
    //while (rs.next()) {
      //System.out.println(rs.getString(1));
    //}

    con.close();
  }


}

