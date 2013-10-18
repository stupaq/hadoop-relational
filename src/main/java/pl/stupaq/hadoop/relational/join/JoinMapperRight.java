package pl.stupaq.hadoop.relational.join;

public class JoinMapperRight extends JoinMapper {

  @Override
  protected String getJoinKeyIndicesKey() {
    return JOIN_KEY_INDICES_PREFIX + "right";
  }

  @Override
  protected boolean isLHS() {
    return false;
  }
}
