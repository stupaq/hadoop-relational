package pl.stupaq.hadoop.relational.aggregation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import pl.stupaq.hadoop.relational.MRClusterTestUtil;
import pl.stupaq.hadoop.relational.aggregation.Aggregator.Sum;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationMRClusterTest extends MRClusterTestUtil {
  private static final Path INPUT = new Path("input");
  private static final Path OUTPUT = new Path("output");
  private static final Path MERGED_OUTPUT = new Path("merged_output");

  @Before
  public void setUp() throws IOException {
    try (Writer writer = new OutputStreamWriter(dfs.create(INPUT))) {
      writer.write("1,2,3\n1,2,4\n2,3,4\n3,4,6\n4,5,6\n3,4,2\n6,4,3\n1,4,3");
    }
  }

  @Test
  public void testRun() throws Exception {
    Configuration conf = mrCluster.createJobConf();
    String[] args = new String[]{INPUT.toString(), OUTPUT.toString(), "0,1", Sum.class.getName()};
    assertEquals("Job failed!", 0, ToolRunner.run(conf, new Aggregation(), args));
    assertTrue(dfs.exists(OUTPUT));
    FileUtil.copyMerge(dfs, OUTPUT, dfs, MERGED_OUTPUT, false, conf, null);
    try (Reader reader = new InputStreamReader(dfs.open(MERGED_OUTPUT))) {
      char[] data = new char[1024];
      reader.read(data);
      assertEquals("1,2,7.0\n1,4,3.0\n2,3,4.0\n3,4,8.0\n4,5,6.0\n6,4,3.0",
                   String.valueOf(data).trim());
    }
  }
}
