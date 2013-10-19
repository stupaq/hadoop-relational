package pl.stupaq.hadoop.relational.union;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.stupaq.hadoop.relational.Tuple;

import java.io.IOException;

public class UnionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws InterruptedException, IOException {
    Tuple tuple = new Tuple();
    tuple.fromText(value);
    context.write(NullWritable.get(), tuple.toText());
  }
}
