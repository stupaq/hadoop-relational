package pl.stupaq.hadoop.relational.selection;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import pl.stupaq.hadoop.relational.Tuple;
import pl.stupaq.hadoop.relational.Utils;

public interface Predicate {
  /** True if given tuple should be included in the result, false otherwise. */
  public boolean eval(Tuple tuple);

  /** Sets up extra parameters for this predicate. */
  public void setup(Configuration conf, String[] args);

  public static class Any implements Predicate {
    @Override
    public boolean eval(Tuple tuple) {
      return true;
    }

    @Override
    public void setup(Configuration conf, String[] args) {
    }
  }

  @SuppressWarnings("unused")
  public static class Equals implements Predicate, Configurable {
    public static final String PREDICATE_EQUALS_TUPLE = "relational.predicate.equals.tuple";
    private Tuple tuple;

    @Override
    public boolean eval(Tuple tuple) {
      return this.tuple.equals(tuple);
    }

    @Override
    public void setup(Configuration conf, String[] args) {
      Utils.checkArgument(args.length > 3, "Missing pattern tuple for Predicate::Equals");
      conf.set(Equals.PREDICATE_EQUALS_TUPLE, args[3]);
    }

    @Override
    public void setConf(Configuration conf) {
      if (conf != null) {
        String description = conf.get(PREDICATE_EQUALS_TUPLE);
        tuple = new Tuple();
        tuple.fromText(new Text(description));
      }
    }

    @Override
    public Configuration getConf() {
      return null;
    }
  }
}
