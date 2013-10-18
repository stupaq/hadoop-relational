package pl.stupaq.hadoop.relational.join;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

public class MRClusterTestUtil {
  protected static final Log LOG = LogFactory.getLog(JoinerMRClusterTest.class);
  protected DistributedFileSystem dfs;
  protected MiniMRCluster mrCluster;
  private MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setUpClass() {
    String buildDir = System.getProperty("project.build.directory");
    System.setProperty("hadoop.log.dir", buildDir + "test/logs");
  }

  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    dfsCluster = new MiniDFSCluster(conf, 1, true, null);
    try {
      dfs = (DistributedFileSystem) dfsCluster.getFileSystem();
      mrCluster = new MiniMRCluster(1, dfs.getUri().toString(), 1);
    } catch (IOException e) {
      tearDown();
      throw e;
    }
  }

  @After
  public void tearDown() {
    IOUtils.cleanup(LOG, dfs);
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }
}
