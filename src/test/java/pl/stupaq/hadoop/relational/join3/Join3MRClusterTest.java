package pl.stupaq.hadoop.relational.join3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import pl.stupaq.hadoop.relational.MRClusterTestUtil;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Join3MRClusterTest extends MRClusterTestUtil {
  private static final Path LEFT = new Path("input1");
  private static final Path MIDDLE = new Path("input2");
  private static final Path RIGHT = new Path("input3");
  private static final Path JOB_OUTPUT = new Path("/tmp/job_output");
  private static final Path OUTPUT = new Path("output");
  private static final int concurrencyLevel = 2;

  @Before
  public void setUp() throws IOException {
    try (Writer writer = new OutputStreamWriter(dfs.create(LEFT))) {
      writer.write("1,2\n3,2\n2,3\n4,2");
    }
    try (Writer writer = new OutputStreamWriter(dfs.create(MIDDLE))) {
      writer.write("1,2\n2,4\n2,3\n3,3");
    }
    try (Writer writer = new OutputStreamWriter(dfs.create(RIGHT))) {
      writer.write("4,3\n2,7\n3,2");
    }
  }

  @Test
  public void testRun() throws Exception {
    Configuration conf = createJobConf();
    String[] args =
        new String[]{LEFT.toString(), MIDDLE.toString(), RIGHT.toString(), JOB_OUTPUT.toString(),
                     "1", "0", "0", String.valueOf(concurrencyLevel)};
    assertEquals("Job failed!", 0, ToolRunner.run(conf, new Join3(), args));
    assertTrue(dfs.exists(JOB_OUTPUT));
    FileUtil.copyMerge(dfs, JOB_OUTPUT, dfs, OUTPUT, true, conf, null);
    try (Reader reader = new InputStreamReader(dfs.open(OUTPUT))) {
      char[] data = new char[1024];
      reader.read(data);
      assertEquals("2,3,3,2\n" + "1,2,3,2\n" + "3,2,3,2\n" + "4,2,3,2\n" + "1,2,4,3\n" + "3,2,4,3\n"
                   + "4,2,4,3", String.valueOf(data).trim());
    }
  }
}
