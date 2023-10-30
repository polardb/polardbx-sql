drop java function if exists myplus|
CREATE JAVA FUNCTION myplus
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint,bigint
  CODE
  public class Myplus extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    Long b = (Long) args[1];
    return a+b;
  }
};
END_CODE|