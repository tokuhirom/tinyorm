package me.geso.tinyorm.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <pre>{@code
 * 		{@literal @}BeforeUpdate
 * 		public static void beforeInsert(UpdateRowStatement stmt) {
 * 			log.info("BEFORE UPDATE");
 * 			stmt.set("y", "fuga");
 * 		}
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface BeforeUpdate {

}
