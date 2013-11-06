package pl.stupaq.hadoop.relational.join3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pl.stupaq.hadoop.relational.MarkedTuple;
import pl.stupaq.hadoop.relational.join3.Join3Mapper.Join3MapperLeft;
import pl.stupaq.hadoop.relational.join3.Join3Mapper.Join3MapperMiddle;
import pl.stupaq.hadoop.relational.join3.Join3Mapper.Join3MapperRight;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Join3 implements Tool {
  static final String JOIN_REDUCERS_SQUARE_ROOT_KEY = "relational.join3.reducers-square-root";
  private Configuration conf;

  public static void main(String[] args) throws Exception {
    try {
      Configuration conf = new Configuration();
      String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      ToolRunner.run(conf, new Join3(), remainingArgs);
    } catch (Throwable t) {
      System.err.println(StringUtils.stringifyException(t));
      throw t;
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    System.out.println(Arrays.asList(args).toString());
    // Parse arguments
    Path leftRelationPath = new Path(args[0]),
        middleRelationPath = new Path(args[1]),
        rightRelationPath = new Path(args[2]),
        outputRelationPath = new Path(args[3]);
    conf.set(new Join3MapperLeft().getJoinKeyIndicesKey(), args[4]);
    // The convention here is that the set of vertices as defined by args[4] determines attributes
    // for left join, the rest of attributes are used for right join.
    conf.set(new Join3MapperMiddle().getJoinKeyIndicesKey(), args[5]);
    conf.set(new Join3MapperRight().getJoinKeyIndicesKey(), args[6]);
    // Number of reducers is equal to this parameter squared
    int reducersSquareRoot = Integer.parseInt(args[7]);
    conf.setInt(JOIN_REDUCERS_SQUARE_ROOT_KEY, reducersSquareRoot);

    // Setup job
    Job job = Job.getInstance(conf);
    job.setJarByClass(Join3.class);

    MultipleInputs
        .addInputPath(job, leftRelationPath, TextInputFormat.class, Join3MapperLeft.class);
    MultipleInputs
        .addInputPath(job, middleRelationPath, TextInputFormat.class, Join3MapperMiddle.class);
    MultipleInputs
        .addInputPath(job, rightRelationPath, TextInputFormat.class, Join3MapperRight.class);

    job.setMapOutputKeyClass(ElementDescriptor.class);
    job.setMapOutputValueClass(MarkedTuple.class);

    job.setPartitionerClass(Join3Partitioner.class);

    job.setReducerClass(Join3Reducer.class);
    job.setNumReduceTasks(reducersSquareRoot * reducersSquareRoot);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputRelationPath);

    // Run job
    job.submit();
    return job.waitForCompletion(true) ? 0 : 1;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration entries) {
    this.conf = entries;
  }

  static class ElementDescriptor implements Writable, WritableComparable<ElementDescriptor> {
    public long i, j;

    public ElementDescriptor() {
    }

    public ElementDescriptor(long i, long j) {
      this.i = i;
      this.j = j;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeLong(i);
      dataOutput.writeLong(j);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      i = dataInput.readLong();
      j = dataInput.readLong();
    }

    @Override
    public int compareTo(ElementDescriptor o) {
      int res = Long.valueOf(i).compareTo(o.i);
      return res != 0 ? res : Long.valueOf(j).compareTo(o.j);
    }

    @Override
    public String toString() {
      return "ElementDescriptor{" +
             "i=" + i +
             ", j=" + j +
             '}';
    }
  }
}
