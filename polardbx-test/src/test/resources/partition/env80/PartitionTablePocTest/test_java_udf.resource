drop java function if exists java_udf_concat|
CREATE JAVA FUNCTION `java_udf_concat` return_type varchar(255) input_types int, varchar(255)
code
public class Java_udf_concat extends UserDefinedJavaFunction {
public Object compute(Object[] args) {
Integer a = (Integer) args[0];
String b = (String) args[1];
return "polarx_" + b + "_" + a;
}
};
end_code|