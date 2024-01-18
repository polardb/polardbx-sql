drop java function if exists myplus3|
CREATE JAVA FUNCTION myplus3
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint,bigint
  CODE
  public class Myplus3 extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    Long b = (Long) args[1];
    return a+b;
  }
};
END_CODE|