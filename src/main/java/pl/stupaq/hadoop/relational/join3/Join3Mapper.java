package pl.stupaq.hadoop.relational.join3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.stupaq.hadoop.relational.MarkedTuple;
import pl.stupaq.hadoop.relational.MarkedTuple.OriginTable;
import pl.stupaq.hadoop.relational.Tuple;
import pl.stupaq.hadoop.relational.Utils;
import pl.stupaq.hadoop.relational.join3.Join3.ElementDescriptor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public abstract class Join3Mapper
    extends Mapper<LongWritable, Text, ElementDescriptor, MarkedTuple> {
  protected static final String JOIN_KEY_INDICES_PREFIX = "relational.join.key_indices.";
  protected List<Integer> joinKeyIndices;
  protected int reducersSquareRoot;

  public static List<Integer> getJoinKeyIndices(Configuration conf, String key) {
    String joinKeyStr = conf.get(key);
    Utils.checkState(joinKeyStr != null, key + " is not set");
    return Collections.unmodifiableList(Utils.parseIntegers(joinKeyStr));
  }

  @Override
  protected final void setup(Context context) throws IllegalStateException {
    Configuration conf = context.getConfiguration();
    joinKeyIndices = getJoinKeyIndices(conf, getJoinKeyIndicesKey());
    reducersSquareRoot = conf.getInt(Join3.JOIN_REDUCERS_SQUARE_ROOT_KEY, -1);
    Utils.checkState(reducersSquareRoot > 0, "Bad reducers square root");
  }

  @Override
  protected abstract void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException;

  protected abstract String getJoinKeyIndicesKey();

  public static class Join3MapperLeft extends Join3Mapper {

    @Override
    protected String getJoinKeyIndicesKey() {
      return JOIN_KEY_INDICES_PREFIX + "left";
    }

    @Override
    protected final void map(LongWritable inputKey, Text value, Context context)
        throws IOException, InterruptedException {
      // Parse input tuple
      Tuple reduceValue = new Tuple();
      reduceValue.fromText(value);
      // Determine join indices
      Tuple keyLeft = reduceValue.project(joinKeyIndices);
      // This tuple must get to all reducers of the form (h(b), x) for x in hash function's image
      for (int i = 0; i < reducersSquareRoot; i++) {
        ElementDescriptor key = new ElementDescriptor(keyLeft.hashCode() % reducersSquareRoot, i);
        context.write(key, new MarkedTuple(reduceValue, OriginTable.LEFT));
      }
    }
  }

  public static class Join3MapperMiddle extends Join3Mapper {

    @Override
    protected String getJoinKeyIndicesKey() {
      return JOIN_KEY_INDICES_PREFIX + "middle";
    }

    @Override
    protected final void map(LongWritable inputKey, Text value, Context context)
        throws IOException, InterruptedException {
      // Parse input tuple
      Tuple reduceValue = new Tuple();
      reduceValue.fromText(value);
      // Determine join indices
      // The convention is that for middle tuple join indices determine left join attributes and
      // the rest of a tuple determines right join attributes.
      Tuple keyLeft = reduceValue.project(joinKeyIndices);
      Tuple keyRight = reduceValue.strip(joinKeyIndices);
      // This tuple will get to a single reducer, write a single copy with key equal to (h(b), h(c))
      ElementDescriptor key = new ElementDescriptor(keyLeft.hashCode() % reducersSquareRoot,
                                                    keyRight.hashCode() % reducersSquareRoot);
      context.write(key, new MarkedTuple(reduceValue, OriginTable.MIDDLE));
    }
  }

  public static class Join3MapperRight extends Join3Mapper {

    @Override
    protected String getJoinKeyIndicesKey() {
      return JOIN_KEY_INDICES_PREFIX + "right";
    }

    @Override
    protected final void map(LongWritable inputKey, Text value, Context context)
        throws IOException, InterruptedException {
      // Parse input tuple
      Tuple reduceValue = new Tuple();
      reduceValue.fromText(value);
      // Determine join indices
      Tuple keyRight = reduceValue.project(joinKeyIndices);
      // This tuple must get to all reducers of the form (x, h(c)) for x in hash function's image
      for (int i = 0; i < reducersSquareRoot; i++) {
        ElementDescriptor key = new ElementDescriptor(i, keyRight.hashCode() % reducersSquareRoot);
        context.write(key, new MarkedTuple(reduceValue, OriginTable.RIGHT));
      }
    }
  }
}
