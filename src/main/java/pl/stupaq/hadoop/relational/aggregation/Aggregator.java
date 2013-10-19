package pl.stupaq.hadoop.relational.aggregation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import pl.stupaq.hadoop.relational.Tuple;

public interface Aggregator {
  /** Groups given tuples into single tuple. */
  public Tuple eval(Iterable<Tuple> tuples);

  /** Sets up extra parameters for this aggregator. */
  public void setup(Configuration conf, String[] args);

  public static class First implements Aggregator {
    @Override
    public Tuple eval(Iterable<Tuple> tuples) {
      return tuples.iterator().next();
    }

    @Override
    public void setup(Configuration conf, String[] args) {
    }
  }

  public static class Sum implements Aggregator {
    @Override
    public Tuple eval(Iterable<Tuple> tuples) {
      double sum = 0D;
      for (Tuple t : tuples) {
        sum += Double.parseDouble(t.get(0));
      }
      Tuple result = new Tuple();
      result.fromText(new Text(String.valueOf(sum)));
      return result;
    }

    @Override
    public void setup(Configuration conf, String[] args) {
    }
  }
}
