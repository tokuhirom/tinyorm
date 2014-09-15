package me.geso.tinyorm.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <pre>
 * <code>
 * 		@BeforeUpdate
 * 		public static void beforeInsert(UpdateRowStatement stmt) {
 * 			log.info("BEFORE UPDATE");
 * 			stmt.set("y", "fuga");
 * 		}
 * </code>
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface BeforeUpdate {

}
