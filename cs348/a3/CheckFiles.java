import java.util.*;
import java.io.*;

public class CheckFiles {
  public static void main(String[] args) throws IOException {
    BufferedReader in1 = new BufferedReader(new FileReader(args[0]));
    BufferedReader in2 = new BufferedReader(new FileReader(args[1]));
    String line1 = in1.readLine();
    String line2 = in2.readLine();

    boolean correct = true;
    while(line1 != null) {
      System.out.print(line1);
      System.out.print(" = ");
      System.out.println(line2);
      if(line1.equals(line2)) {
        System.out.println("true");
      } else {
        System.out.println("BAD");
        correct = false;
        break;
      }

      line1 = in1.readLine();
      line2 = in2.readLine();
    }

    in1.close();
    in2.close();

    if(correct) {
      System.out.println("TESTS PASSED");
    } else {
      System.out.println("TESTS FAILED");
    }
  }
}
