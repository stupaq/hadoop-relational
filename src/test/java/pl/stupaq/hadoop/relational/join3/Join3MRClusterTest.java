package pl.stupaq.hadoop.relational.join3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
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
  private static final int CONCURRENCY_LEVEL = 2;

  @Test
  public void testRun() throws Exception {
    new TestRun("1,2\n3,2\n2,3\n4,2", "1,2\n2,4\n2,3\n3,3", "4,3\n2,7\n3,2",
                "2,3,3,2\n1,2,3,2\n3,2,3,2\n4,2,3,2\n1,2,4,3\n3,2,4,3\n4,2,4,3");

    new TestRun("1,2\n3,2\n2,3\n4,1", "1,2\n3,3\n2,3\n3,3", "4,3\n2,7\n3,2",
                "2,3,3,2\n2,3,3,2\n4,1,2,7\n1,2,3,2\n3,2,3,2");

    new TestRun("1,2\n3,2\n2,3\n1,2", "1,2\n2,4\n2,3\n3,3", "4,3\n0,7\n5,2",
                "1,2,4,3\n3,2,4,3\n1,2,4,3");
  }

  protected class TestRun {
    private final Configuration conf;

    public TestRun(String input1, String input2, String input3, String output) throws Exception {
      LOG.info("TestRun:\ninput1:\n" + input1 + "\ninput2:\n" + input2 + "\ninput3:\n" + input3);
      this.conf = createJobConf();
      try {
        writeInputs(input1, input2, input3);
        runTest(CONCURRENCY_LEVEL);
        checkOutput(output);
      } finally {
        cleanup();
      }
    }

    private void writeInputs(String input1, String input2, String input3) throws IOException {
      try (Writer writer = new OutputStreamWriter(dfs.create(LEFT, true))) {
        writer.write(input1);
      }
      try (Writer writer = new OutputStreamWriter(dfs.create(MIDDLE, true))) {
        writer.write(input2);
      }
      try (Writer writer = new OutputStreamWriter(dfs.create(RIGHT, true))) {
        writer.write(input3);
      }
    }

    private void runTest(int concurrencyLevel) throws Exception {
      String[] args =
          new String[]{LEFT.toString(), MIDDLE.toString(), RIGHT.toString(), JOB_OUTPUT.toString(),
                       "1", "0", "0", String.valueOf(concurrencyLevel)};
      assertEquals("Job failed!", 0, ToolRunner.run(conf, new Join3(), args));
    }

    private void checkOutput(String output) throws IOException {
      assertTrue(dfs.exists(JOB_OUTPUT));
      FileUtil.copyMerge(dfs, JOB_OUTPUT, dfs, OUTPUT, true, conf, null);
      try (Reader reader = new InputStreamReader(dfs.open(OUTPUT))) {
        char[] data = new char[1024];
        reader.read(data);
        assertEquals(output, String.valueOf(data).trim());
      }
    }

    private void cleanup() throws IOException {
      dfs.delete(LEFT, true);
      dfs.delete(MIDDLE, true);
      dfs.delete(RIGHT, true);
      dfs.delete(OUTPUT, true);
    }
  }
}
