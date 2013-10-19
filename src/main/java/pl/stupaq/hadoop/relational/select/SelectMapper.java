package pl.stupaq.hadoop.relational.select;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import pl.stupaq.hadoop.relational.Tuple;
import pl.stupaq.hadoop.relational.select.Predicate.Any;

import java.io.IOException;

public class SelectMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
  private Predicate predicate;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Class<? extends Predicate> clazz =
        conf.getClass(Select.SELECT_PREDICATE_CLASS_KEY, Any.class, Predicate.class);
    predicate = ReflectionUtils.newInstance(clazz, conf);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    Tuple tuple = new Tuple();
    tuple.fromText(value);
    if (predicate.eval(tuple)) {
      context.write(NullWritable.get(), tuple.toText());
    }
  }
}
