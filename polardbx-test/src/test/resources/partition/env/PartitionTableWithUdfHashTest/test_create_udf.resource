drop java function if exists myfunc1|
CREATE JAVA FUNCTION myfunc1
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
  CODE
  public class Myfunc1 extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    return a;
  }
};
END_CODE|

drop java function if exists myfunc2|
CREATE JAVA FUNCTION myfunc2
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint,bigint
  CODE
  public class Myfunc2 extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    Long b = (Long) args[1];
    return a+b;
  }
};
END_CODE|