drop java function if exists myplus2|
CREATE JAVA FUNCTION myplus2
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint,bigint
  CODE
  public class Myplus2 extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    Long b = (Long) args[1];
    return a+b;
  }
};
END_CODE|