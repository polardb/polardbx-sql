drop java function if exists Substrforint|
CREATE JAVA FUNCTION Substrforint
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint, bigint
CODE
  public class Substrforint extends UserDefinedJavaFunction {
    public Object compute(Object[] args) {
        Long val = (Long) args[0];
        if(val == null) {
            return 0;
        }
        String str = String.valueOf(val);
        Long len = (Long) args[1];
        try {
            Integer n = len.intValue();
            if (n >= str.length()) {
                return Long.valueOf(str);
            }
            int startIndex = str.length() - n;
            String ss = str.substring(startIndex);
            return Long.valueOf(ss);
        } catch (Throwable ex) {
             return 0;
        }
    }
  };
END_CODE|

drop java function if exists Substrforvar|
CREATE JAVA FUNCTION Substrforvar
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(64), bigint
CODE
  public class Substrforvar extends UserDefinedJavaFunction {
    public Object compute(Object[] args) {
        String str = (String) args[0];
        if(str == null) {
            return 0;
        }
        Long len = (Long) args[1];
        try {
            Integer n = len.intValue();
            if (n >= str.length()) {
                return Long.valueOf(str);
            }
            int startIndex = str.length() - n;
            String ss = str.substring(startIndex);
            return Long.valueOf(ss);
        } catch (Throwable ex) {
             return 0;
        }
    }
  };
END_CODE|