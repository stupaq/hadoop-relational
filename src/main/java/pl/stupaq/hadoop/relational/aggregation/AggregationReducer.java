package pl.stupaq.hadoop.relational.aggregation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import pl.stupaq.hadoop.relational.Tuple;
import pl.stupaq.hadoop.relational.aggregation.Aggregator.First;

import java.io.IOException;

public class AggregationReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
  private Aggregator aggregator;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Class<? extends Aggregator> clazz =
        conf.getClass(Aggregation.AGGREGATION_AGGREGATOR_CLASS_KEY, First.class, Aggregator.class);
    aggregator = ReflectionUtils.newInstance(clazz, conf);
  }

  @Override
  protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
      throws IOException, InterruptedException {
    Tuple value = aggregator.eval(values);
    key.append(value);
    context.write(NullWritable.get(), key.toText());
  }
}
