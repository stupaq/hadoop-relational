package pl.stupaq.hadoop.relational;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MarkedTuple implements Writable {
  private Tuple tuple;
  private OriginTable origin;

  @SuppressWarnings("unused")
  public MarkedTuple() {
    tuple = new Tuple();
  }

  public MarkedTuple(Tuple tuple, OriginTable origin) {
    this.tuple = tuple;
    this.origin = origin;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    tuple.write(dataOutput);
    WritableUtils.writeEnum(dataOutput, origin);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    tuple.readFields(dataInput);
    origin = WritableUtils.readEnum(dataInput, OriginTable.class);
  }

  public OriginTable getOrigin() {
    return origin;
  }

  public Tuple getTuple() {
    return tuple;
  }

  public static enum OriginTable {
    LEFT,
    MIDDLE,
    RIGHT;
  }
}
