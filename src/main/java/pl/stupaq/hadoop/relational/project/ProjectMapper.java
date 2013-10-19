package pl.stupaq.hadoop.relational.project;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.stupaq.hadoop.relational.Tuple;
import pl.stupaq.hadoop.relational.Utils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ProjectMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
  protected List<Integer> projectionIndices;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    String joinKeyStr = context.getConfiguration().get(Project.PROJECT_INDICES_LEY);
    Preconditions.checkState(joinKeyStr != null, Project.PROJECT_INDICES_LEY + " is not set");
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
