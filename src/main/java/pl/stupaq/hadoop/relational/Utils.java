package pl.stupaq.hadoop.relational;

import java.util.ArrayList;
import java.util.List;

public final class Utils {
  private static final String STRING_LIST_DELIMITER = ",";

  private Utils() {
  }

  public static List<Integer> parseIntegers(String str) {
    List<Integer> result = new ArrayList<Integer>();
    for (String elemStr : str.split(STRING_LIST_DELIMITER)) {
      result.add(Integer.parseInt(elemStr));
    }
    return result;
  }
}
