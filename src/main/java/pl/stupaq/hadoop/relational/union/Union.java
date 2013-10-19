package pl.stupaq.hadoop.relational.union;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Union implements Tool {
  private Configuration conf;

  public static void main(String[] args) throws Exception {
    try {
      Configuration conf = new Configuration();
      String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      ToolRunner.run(conf, new Union(), remainingArgs);
    } catch (Throwable t) {
      System.err.println(StringUtils.stringifyException(t));
      throw t;
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    // Parse arguments
    Path oneRelationPath = new Path(args[0]),
        twoRelationPath = new Path(args[1]),
        outputRelationPath = new Path(args[2]);

    // Setup job
    Job job = Job.getInstance(conf);
    job.setJarByClass(Union.class);

    MultipleInputs.addInputPath(job, oneRelationPath, TextInputFormat.class, UnionMapper.class);
    MultipleInputs.addInputPath(job, twoRelationPath, TextInputFormat.class, UnionMapper.class);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);

    // This is a map-only job
    job.setNumReduceTasks(0);

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
}
