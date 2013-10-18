package pl.stupaq.hadoop.relational.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JoinerMRClusterTest extends MRClusterTestUtil {
  private static final Path inputLeft = new Path("left");
  private static final Path inputRight = new Path("right");
  private static final Path output = new Path("output");
  private static final Path mergedOutput = new Path("merged_output");

  @Before
  public void setUp() throws IOException {
    try (Writer writer = new OutputStreamWriter(dfs.create(inputLeft))) {
      writer.write("1,2,3\n1,2,4\n2,3,4\n4,2,3");
    }
    try (Writer writer = new OutputStreamWriter(dfs.create(inputRight))) {
      writer.write("1,5,3\n1,2,7\n4,3,2");
    }
  }

  @Test
  public void testRun() throws Exception {
    Configuration conf = mrCluster.createJobConf();
    String[] args =
        new String[]{inputLeft.toString(), inputRight.toString(), output.toString(), "0", "0"};
    assertEquals("Job failed!", 0, ToolRunner.run(conf, new Joiner(), args));
    assertTrue(dfs.exists(output));
    FileUtil.copyMerge(dfs, output, dfs, mergedOutput, false, conf, null);
    try (Reader reader = new InputStreamReader(dfs.open(mergedOutput))) {
      char[] data = new char[1024];
      reader.read(data);
      assertEquals("1,2,3,5,3\n1,2,3,2,7\n1,2,4,5,3\n1,2,4,2,7\n4,2,3,3,2",
                   String.valueOf(data).trim());
    }
  }
}
