package pl.stupaq.hadoop.relational.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pl.stupaq.hadoop.relational.Tuple;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Join implements Tool {
  private Configuration conf;

  public static void main(String[] args) throws Exception {
    try {
      Configuration conf = new Configuration();
      String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      ToolRunner.run(conf, new Join(), remainingArgs);
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
        rightRelationPath = new Path(args[1]),
        outputRelationPath = new Path(args[2]);
    conf.set(new JoinMapperLeft().getJoinKeyIndicesKey(), args[3]);
    conf.set(new JoinMapperRight().getJoinKeyIndicesKey(), args[4]);

    // Setup job
    Job job = Job.getInstance(conf);
    job.setJarByClass(Join.class);

    MultipleInputs.addInputPath(job, leftRelationPath, TextInputFormat.class, JoinMapperLeft.class);
    MultipleInputs
        .addInputPath(job, rightRelationPath, TextInputFormat.class, JoinMapperRight.class);

    job.setMapOutputKeyClass(Tuple.class);
    job.setMapOutputValueClass(MarkedTuple.class);

    job.setReducerClass(JoinReducer.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputRelationPath);

    // Run job
    job.submit();
    return job.waitForCompletion(true) ? 0 : 1;
  }

  protected static class MarkedTuple implements Writable {
    private Tuple tuple;
    private boolean LHS;

    @SuppressWarnings("unused")
    public MarkedTuple() {
      tuple = new Tuple();
    }

    public MarkedTuple(Tuple tuple, boolean LHS) {
      this.tuple = tuple;
      this.LHS = LHS;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      tuple.write(dataOutput);
      dataOutput.writeBoolean(LHS);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      tuple.readFields(dataInput);
      LHS = dataInput.readBoolean();
    }

    public boolean isLHS() {
      return LHS;
    }

    public Tuple getTuple() {
      return tuple;
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration entries) {
    this.conf = entries;
  }
}
