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
  private static final Path LEFT = new Path("left");
  private static final Path RIGHT = new Path("right");
  private static final Path OUTPUT = new Path("output");
  private static final Path MERGED_OUTPUT = new Path("merged_output");

  @Before
  public void setUp() throws IOException {
    try (Writer writer = new OutputStreamWriter(dfs.create(LEFT))) {
      writer.write("1,2,3\n1,2,4\n2,3,4\n4,2,3");
    }
    try (Writer writer = new OutputStreamWriter(dfs.create(RIGHT))) {
      writer.write("1,5,3\n1,2,7\n4,3,2");
    }
  }

  @Test
  public void testRun() throws Exception {
    Configuration conf = mrCluster.createJobConf();
    String[] args = new String[]{LEFT.toString(), RIGHT.toString(), OUTPUT.toString(), "0", "0"};
    assertEquals("Job failed!", 0, ToolRunner.run(conf, new Join(), args));
    assertTrue(dfs.exists(OUTPUT));
    FileUtil.copyMerge(dfs, OUTPUT, dfs, MERGED_OUTPUT, false, conf, null);
    try (Reader reader = new InputStreamReader(dfs.open(MERGED_OUTPUT))) {
      char[] data = new char[1024];
      reader.read(data);
      assertEquals("1,2,3,5,3\n1,2,3,2,7\n1,2,4,5,3\n1,2,4,2,7\n4,2,3,3,2",
                   String.valueOf(data).trim());
    }
  }
}
