drop java function if exists myf1bintbint2bint|
CREATE JAVA FUNCTION myf1bintbint2bint
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint,bigint
CODE
  public class Myf1bintbint2bint extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    Long b = (Long) args[1];
    return a + b;
  }
};
END_CODE|

drop java function if exists myf1bintbint2var|
CREATE JAVA FUNCTION myf1bintbint2var
  no state
  RETURN_TYPE varchar(255)
  INPUT_TYPES bigint, bigint
CODE
  public class Myf1bintbint2var extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    Long b = (Long) args[1];
    return (a+b)+"";
  }
};
END_CODE|


drop java function if exists myf1varvar2bint|
CREATE JAVA FUNCTION myf1varvar2bint
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(255),varchar(255)
CODE
  public class Myf1varvar2bint extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    String a = (String) args[0];
    Long aVal = Long.valueOf(a);
    String b = (String) args[1];
    Long bVal = Long.valueOf(b);
    return aVal + bVal;
  }
};
END_CODE|

drop java function if exists myf1varvar2var|
CREATE JAVA FUNCTION myf1varvar2var
  no state
  RETURN_TYPE varchar(255)
  INPUT_TYPES varchar(255),varchar(255)
 CODE
  public class Myf1varvar2var extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    String a = (String) args[0];
    String b = (String) args[1];
    return a+b;
  }
};
END_CODE|
