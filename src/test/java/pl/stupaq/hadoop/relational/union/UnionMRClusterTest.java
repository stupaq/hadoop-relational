package pl.stupaq.hadoop.relational.union;

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

public class UnionMRClusterTest extends MRClusterTestUtil {
  private static final Path ONE = new Path("one");
  private static final Path TWO = new Path("two");
  private static final Path OUTPUT = new Path("output");
  private static final Path MERGED_OUTPUT = new Path("merged_output");

  @Before
  public void setUp() throws IOException {
    try (Writer writer = new OutputStreamWriter(dfs.create(ONE))) {
      writer.write("1,2,3\n1,2,4\n2,3,4\n4,2,3");
    }
    try (Writer writer = new OutputStreamWriter(dfs.create(TWO))) {
      writer.write("1,5,3\n1,2,7\n4,3,2");
    }
  }

  @Test
  public void testRun() throws Exception {
    Configuration conf = mrCluster.createJobConf();
    String[] args = new String[]{ONE.toString(), TWO.toString(), OUTPUT.toString(), "0", "0"};
    assertEquals("Job failed!", 0, ToolRunner.run(conf, new Union(), args));
    assertTrue(dfs.exists(OUTPUT));
    FileUtil.copyMerge(dfs, OUTPUT, dfs, MERGED_OUTPUT, false, conf, null);
    try (Reader reader = new InputStreamReader(dfs.open(MERGED_OUTPUT))) {
      char[] data = new char[1024];
      reader.read(data);
      assertEquals("1,2,3\n1,2,4\n2,3,4\n4,2,3\n1,5,3\n1,2,7\n4,3,2", String.valueOf(data).trim());
    }
  }
}
