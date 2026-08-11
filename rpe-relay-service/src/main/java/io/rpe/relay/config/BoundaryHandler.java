package io.rpe.relay.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a code unit whose broad {@code catch (Exception|Throwable)} is a deliberate
 * last-line-of-defense at a thread / loop / listener / scheduler boundary — NOT lazy
 * domain-logic catching. {@code value} states WHY the broad catch is correct here.
 *
 * <p>Enforced by the ArchUnit rule {@code broadCatchOnlyInBoundaryHandlers} (RelayArchTest):
 * a broad catch is a build failure unless its enclosing method (or class) carries this
 * annotation. The ladder is: specific exception → else {@code RuntimeException} (allowed,
 * narrower) → else {@code Exception/Throwable} which requires this annotation. Per-service
 * copy (no shared rpe-common jar), symmetric with {@code Pii} / {@code KafkaSecurity}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.TYPE})
public @interface BoundaryHandler {
    /** Why broad catch is the correct last resort, e.g. "relay-loop-must-survive". */
    String value();
}
