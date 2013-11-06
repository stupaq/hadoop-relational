package pl.stupaq.hadoop.relational.join3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

import pl.stupaq.hadoop.relational.MarkedTuple;
import pl.stupaq.hadoop.relational.Utils;
import pl.stupaq.hadoop.relational.join3.Join3.ElementDescriptor;

public class Join3Partitioner extends Partitioner<ElementDescriptor, MarkedTuple>
    implements Configurable {
  private static final Log LOG = LogFactory.getLog(Join3Partitioner.class);
  private Configuration conf;
  private int reducersSquareRoot;

  @Override
  public int getPartition(ElementDescriptor key, MarkedTuple value, int reducersCount) {
    int partition = (int) (key.i * reducersSquareRoot + key.j) % reducersCount;
    LOG.debug("Key: " + key + " sent to partition: " + partition);
    return partition;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.reducersSquareRoot = this.conf.getInt(Join3.JOIN_REDUCERS_SQUARE_ROOT_KEY, -1);
    Utils.checkState(this.reducersSquareRoot > 0, "Bad reducers square root");
  }
}
