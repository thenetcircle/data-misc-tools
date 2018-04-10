package annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.time.Instant;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ProcDescription {
    String description() default "not-described";

    /**
     * refer to {@link Duration}
     * @return String
     */
    String interval() default "PT1M";
}
