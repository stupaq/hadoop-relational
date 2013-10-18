package pl.stupaq.hadoop.relational.join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pl.stupaq.hadoop.relational.Tuple;
import pl.stupaq.hadoop.relational.join.Joiner.MarkedTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinReducer extends Reducer<Tuple, MarkedTuple, NullWritable, Text> {

  @Override
  protected void reduce(Tuple key, Iterable<MarkedTuple> values, Context context)
      throws IOException, InterruptedException {
    List<Tuple> lhsList = new ArrayList<>(), rhsList = new ArrayList<>();
    for (MarkedTuple tuple : values) {
      if (tuple.isLHS()) {
        lhsList.add(new Tuple(tuple.getTuple()));
      } else {
        rhsList.add(new Tuple(tuple.getTuple()));
      }
    }
    for (Tuple lhs : lhsList) {
      for (Tuple rhs : rhsList) {
        Tuple result = new Tuple(key);
        result.append(lhs);
        result.append(rhs);
        context.write(NullWritable.get(), result.toText());
      }
    }
  }
}
