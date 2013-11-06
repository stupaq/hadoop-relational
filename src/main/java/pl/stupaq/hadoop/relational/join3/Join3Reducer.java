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
    // Split all tuples hashed to this reducer according to source tables
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
    // This is a simple 3-nested-loops join.
    // This join can be implemented in a more efficient way, since we expect each reduce split to
    // be small it doesn't matter.
    // Also the size of the output is \Omega(|input|^3) in pessimistic case.
    LOG.info("RES");
    for (Tuple mid : midList) {
      // The convention here is that the list of columns as defined by middle join indices determines
      // attributes for left join, the rest of attributes are used for right join.
      Tuple midKeyLeft = mid.project(middleJoinIndices);
      Tuple midKeyRight = mid.strip(middleJoinIndices);
      for (Tuple lhs : lhsList) {
        Tuple lhsKey = lhs.project(leftJoinIndices);
        Tuple lhsValue = lhs.strip(leftJoinIndices);
        // Match left table join key with left join key of middle relation
        if (!midKeyLeft.equals(lhsKey)) {
          continue;
        }
        for (Tuple rhs : rhsList) {
          Tuple rhsKey = rhs.project(rightJoinIndices);
          Tuple rhsValue = rhs.strip(rightJoinIndices);
          // Match right table join key with right join key of middle relation
          if (midKeyRight.equals(rhsKey)) {
            Tuple result = new Tuple(lhsValue);
            result.append(mid);
            result.append(rhsValue);
            LOG.info(result.toString());
            // Output tuple is a concatenation of left, middle and right relations' tuples
            // (in this order), we do skip repeated columns though, so that the only remaining
            // join keys reside in 'middle' part of the output tuple.
            context.write(NullWritable.get(), result.toText());
          }
        }
      }
    }
  }
}
