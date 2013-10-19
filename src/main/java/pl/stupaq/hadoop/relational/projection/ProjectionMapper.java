package pl.stupaq.hadoop.relational.projection;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.stupaq.hadoop.relational.Tuple;
import pl.stupaq.hadoop.relational.Utils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ProjectionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
  protected List<Integer> projectionIndices;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    String joinKeyStr = context.getConfiguration().get(Projection.PROJECT_INDICES_KEY);
    Utils.checkState(joinKeyStr != null, Projection.PROJECT_INDICES_KEY + " is not set");
    projectionIndices = Collections.unmodifiableList(Utils.parseIntegers(joinKeyStr));
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    Tuple tuple = new Tuple();
    tuple.fromText(value);
    context.write(NullWritable.get(), tuple.project(projectionIndices).toText());
  }
}
