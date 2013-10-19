package pl.stupaq.hadoop.relational;

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

import pl.stupaq.hadoop.relational.join.JoinMRClusterTest;

import java.io.IOException;

public abstract class MRClusterTestUtil {
  protected static final Log LOG = LogFactory.getLog(JoinMRClusterTest.class);
  protected DistributedFileSystem dfs;
  protected MiniMRCluster mrCluster;
  protected MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setUpClusterClass() {
    String buildDir = System.getProperty("project.build.directory");
    if (buildDir == null) {
      buildDir = "build";
    }
    System.setProperty("hadoop.log.dir", buildDir + "/test/logs");
  }

  @Before
  public final void setUpCluster() throws IOException {
    Configuration conf = new Configuration();
    dfsCluster = new MiniDFSCluster(conf, 1, true, null);
    dfs = (DistributedFileSystem) dfsCluster.getFileSystem();
    mrCluster = new MiniMRCluster(1, dfs.getUri().toString(), 1);
  }

  @After
  public final void tearDownCluster() {
    IOUtils.cleanup(LOG, dfs);
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }
}
