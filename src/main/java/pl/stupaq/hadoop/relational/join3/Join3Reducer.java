package pl.stupaq.hadoop.relational.join3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pl.stupaq.hadoop.relational.MarkedTuple;
import pl.stupaq.hadoop.relational.Tuple;
import pl.stupaq.hadoop.relational.join3.Join3.ElementDescriptor;
import pl.stupaq.hadoop.relational.join3.Join3Mapper.Join3MapperLeft;
import pl.stupaq.hadoop.relational.join3.Join3Mapper.Join3MapperMiddle;
import pl.stupaq.hadoop.relational.join3.Join3Mapper.Join3MapperRight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Join3Reducer extends Reducer<ElementDescriptor, MarkedTuple, NullWritable, Text> {
  private static final Log LOG = LogFactory.getLog(Join3Reducer.class);
  private List<Integer> leftJoinIndices;
  private List<Integer> middleJoinIndices;
  private List<Integer> rightJoinIndices;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    leftJoinIndices =
        Join3Mapper.getJoinKeyIndices(conf, new Join3MapperLeft().getJoinKeyIndicesKey());
    middleJoinIndices =
        Join3Mapper.getJoinKeyIndices(conf, new Join3MapperMiddle().getJoinKeyIndicesKey());
    rightJoinIndices =
        Join3Mapper.getJoinKeyIndices(conf, new Join3MapperRight().getJoinKeyIndicesKey());
  }

  @Override
  protected void reduce(ElementDescriptor key, Iterable<MarkedTuple> values, Context context)
      throws IOException, InterruptedException {
    List<Tuple> lhsList = new ArrayList<>(), rhsList = new ArrayList<>(), midList =
        new ArrayList<>();
    for (MarkedTuple tuple : values) {
      switch (tuple.getOrigin()) {
        case LEFT:
          lhsList.add(new Tuple(tuple.getTuple()));
          break;
        case RIGHT:
          rhsList.add(new Tuple(tuple.getTuple()));
          break;
        case MIDDLE:
          midList.add(new Tuple(tuple.getTuple()));
          break;
      }
    }
    // Debug information
    LOG.info("LHS");
    for (Tuple tuple : lhsList) {
      LOG.info(tuple.toString());
    }
    LOG.info("MID");
    for (Tuple tuple : midList) {
      LOG.info(tuple.toString());
    }
    LOG.info("RHS");
    for (Tuple tuple : rhsList) {
      LOG.info(tuple.toString());
    }
    // This join can be implemented in a more effective way, since we expect each reduce split to
    // be small it doesn't matter.
    LOG.info("RES");
    for (Tuple mid : midList) {
      // The convention here is that the set of vertices as defined by args[4] determines attributes
      // for left join, the rest of attributes are used for right join.
      Tuple midKeyLeft = mid.project(middleJoinIndices);
      Tuple midKeyRight = mid.strip(middleJoinIndices);
      for (Tuple lhs : lhsList) {
        Tuple lhsKey = lhs.project(leftJoinIndices);
        Tuple lhsValue = lhs.strip(leftJoinIndices);
        if (!midKeyLeft.equals(lhsKey)) {
          continue;
        }
        for (Tuple rhs : rhsList) {
          Tuple rhsKey = rhs.project(rightJoinIndices);
          Tuple rhsValue = rhs.strip(rightJoinIndices);
          if (midKeyRight.equals(rhsKey)) {
            Tuple result = new Tuple(lhsValue);
            result.append(mid);
            result.append(rhsValue);
            LOG.info(result.toString());
            context.write(NullWritable.get(), result.toText());
          }
        }
      }
    }
  }
}
