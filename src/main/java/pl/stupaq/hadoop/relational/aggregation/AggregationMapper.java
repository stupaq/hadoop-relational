package pl.stupaq.hadoop.relational.aggregation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.stupaq.hadoop.relational.Tuple;
import pl.stupaq.hadoop.relational.Utils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class AggregationMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
  protected List<Integer> keyIndices;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    String joinKeyStr = context.getConfiguration().get(Aggregation.AGGREGATION_KEY_INDICES_KEY);
    Utils.checkState(joinKeyStr != null, Aggregation.AGGREGATION_KEY_INDICES_KEY + " is not set");
    keyIndices = Collections.unmodifiableList(Utils.parseIntegers(joinKeyStr));
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws InterruptedException, IOException {
    Tuple reduceValue = new Tuple();
    reduceValue.fromText(value);
    Tuple reduceKey = reduceValue.project(keyIndices);
    reduceValue.strip(keyIndices);
    context.write(reduceKey, reduceValue);
  }
}
