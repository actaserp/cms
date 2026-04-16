package mes.app.aspect;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DecryptField {
    String[] columns(); //복호화 대상 컬럼
    int[] masks() default 0; // 마스킹 자릿수
}
