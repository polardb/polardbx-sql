drop java function if exists myf1bint2bint|
CREATE JAVA FUNCTION myf1bint2bint
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE
  public class Myf1bint2bint extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    return a;
  }
};
END_CODE|

drop java function if exists myf1bint2var|
CREATE JAVA FUNCTION myf1bint2var
  no state
  RETURN_TYPE varchar(255)
  INPUT_TYPES bigint
CODE
  public class Myf1bint2var extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    return a+"";
  }
};
END_CODE|


drop java function if exists myf1var2bint|
CREATE JAVA FUNCTION myf1var2bint
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(255)
CODE
  public class Myf1var2bint extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    String a = (String) args[0];
    Long b = Long.valueOf(a);
    return b;
  }
};
END_CODE|

drop java function if exists myf1var2var|
CREATE JAVA FUNCTION myf1var2var
  no state
  RETURN_TYPE varchar(255)
  INPUT_TYPES varchar(255)
 CODE
  public class Myf1var2var extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    String a = (String) args[0];
    return a;
  }
};
END_CODE|
