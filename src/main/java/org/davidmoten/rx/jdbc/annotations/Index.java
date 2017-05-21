package org.davidmoten.rx.jdbc.annotations;

import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Index {
    /**
     * 1 based index corresponding the index in a
     * <code>ResultSet.getObject(index)</code> call.
     * 
     * @return the 1 based index that the annotated method corresponds to in the
     *         ResultSet
     */
    int value();
}