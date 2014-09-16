package me.geso.tinyorm.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <pre>{@code
 * 		{@literal @}BeforeInsert
 * 		public static void beforeInsert(InsertStatement<Row> statement) {
 * 			stmt.value("y", "fill");
 * 		}
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface BeforeInsert {

}
