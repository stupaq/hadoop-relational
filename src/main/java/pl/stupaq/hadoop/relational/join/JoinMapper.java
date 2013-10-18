package pl.stupaq.hadoop.relational.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.stupaq.hadoop.relational.Tuple;
import pl.stupaq.hadoop.relational.Utils;
import pl.stupaq.hadoop.relational.join.Joiner.MarkedTuple;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public abstract class JoinMapper extends Mapper<LongWritable, Text, Tuple, MarkedTuple> {
  protected static final String JOIN_KEY_INDICES_PREFIX = "relational.join.key_indices.";
  protected List<Integer> joinKeyIndices;

  @Override
  protected final void setup(Context context) throws IllegalStateException {
    String key = getJoinKeyIndicesKey();
    String joinKeyStr = context.getConfiguration().get(key);
    if (joinKeyStr == null) {
      throw new IllegalStateException(key + " is not set");
    }
    joinKeyIndices = Collections.unmodifiableList(Utils.parseIntegers(joinKeyStr));
  }

  @Override
  protected final void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    Tuple reduceValue = new Tuple();
    reduceValue.fromText(value);
    Tuple reduceKey = reduceValue.project(joinKeyIndices);
    reduceValue.strip(joinKeyIndices);
    context.write(reduceKey, new MarkedTuple(reduceValue, isLHS()));
  }

  protected abstract String getJoinKeyIndicesKey();

  protected abstract boolean isLHS();
}
