package pl.stupaq.hadoop.relational.join;

public class JoinMapperLeft extends JoinMapper {

  @Override
  protected String getJoinKeyIndicesKey() {
    return JOIN_KEY_INDICES_PREFIX + "left";
  }

  @Override
  protected boolean isLHS() {
    return true;
  }
}
