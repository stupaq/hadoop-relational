package pl.stupaq.hadoop.relational.aggregation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pl.stupaq.hadoop.relational.Tuple;
import pl.stupaq.hadoop.relational.aggregation.Aggregator.First;

public class Aggregation implements Tool {
  static final String AGGREGATION_KEY_INDICES_KEY = "relational.aggregation.key_indices";
  static final String AGGREGATION_AGGREGATOR_CLASS_KEY = "relational.aggregation.aggregator.class";
  private Configuration conf;

  public static void main(String[] args) throws Exception {
    try {
      Configuration conf = new Configuration();
      String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      ToolRunner.run(conf, new Aggregation(), remainingArgs);
    } catch (Throwable t) {
      System.err.println(StringUtils.stringifyException(t));
      throw t;
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    // Parse arguments
    Path inputRelationPath = new Path(args[0]),
        outputRelationPath = new Path(args[1]);
    conf.set(AGGREGATION_KEY_INDICES_KEY, args[2]);
    conf.set(AGGREGATION_AGGREGATOR_CLASS_KEY, args[3]);
    Class<? extends Aggregator> clazz =
        conf.getClass(Aggregation.AGGREGATION_AGGREGATOR_CLASS_KEY, First.class, Aggregator.class);
    ReflectionUtils.newInstance(clazz, null).setup(conf, args);

    // Setup job
    Job job = Job.getInstance(conf);
    job.setJarByClass(Aggregation.class);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, inputRelationPath);

    job.setMapperClass(AggregationMapper.class);
    job.setMapOutputKeyClass(Tuple.class);
    job.setMapOutputValueClass(Tuple.class);

    // This is a map-only job
    job.setReducerClass(AggregationReducer.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputRelationPath);

    // Run job
    job.submit();
    return job.waitForCompletion(true) ? 0 : 1;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
