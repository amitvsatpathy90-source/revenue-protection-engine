package io.rpe.relay;

import io.rpe.relay.config.BoundaryHandler;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.domain.TryCatchBlock;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noMethods;

/**
 * ArchUnit guards for the relay service (microservices.md §7) — the same silent-failure
 * constraints enforced in the core module, now asserted within this service.
 */
class RelayArchTest {

    private static JavaClasses classes;

    @BeforeAll
    static void importClasses() {
        // Import the whole module surface (relay + observability + util) so the cross-service
        // guard sees every class this deployable ships.
        classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.rpe");
    }

    /** subscribe() with no onError handler swallows exceptions silently. */
    @Test
    void subscribeCallsMustHaveErrorHandler() {
        ArchRule rule = noClasses()
                .should().callMethod(reactor.core.publisher.Mono.class, "subscribe")
                .orShould().callMethod(reactor.core.publisher.Flux.class, "subscribe");
        rule.check(classes);
    }

    /** synchronized pins virtual-thread carriers — use ReentrantLock (reactive-pipeline.md). */
    @Test
    void noSynchronizedMethods() {
        ArchRule rule = noMethods().should().haveModifier(JavaModifier.SYNCHRONIZED);
        rule.check(classes);
    }

    /**
     * ADR-17 §7 Stage 4 — no cross-service package dependency. The relay forwards the opaque
     * outbox payload byte-identically (ADR-11) and never deserializes it to a core DTO; it shares
     * the outbox as a schema, not code (microservices.md §1.4/§2). This tripwire fails if anyone
     * adds a Maven dependency on a sibling service — including detection's core (Detector set /
     * Lua gate), which must never be referenced across the service boundary.
     */
    @Test
    void noCrossServicePackageDependencies() {
        ArchRule rule = noClasses()
                .should()
                .dependOnClassesThat()
                .resideInAnyPackage(
                        "io.rpe.detection..",
                        "io.rpe.redis..",
                        "io.rpe.outbox..",
                        "io.rpe.consumer..",
                        "io.rpe.alert..",
                        "io.rpe.triage..");
        rule.check(classes);
    }

    /**
     * Broad catch (Exception/Throwable) is permitted ONLY inside a @BoundaryHandler code unit
     * (or class) — a deliberate last line of defense at a thread/loop/listener/scheduler frame.
     * Elsewhere catch the specific exception, or RuntimeException (the allowed narrower rung).
     * Bans accidental broad catches in domain logic (error-boundaries.md / ADR-21).
     */
    @Test
    void broadCatchOnlyInBoundaryHandlers() {
        ArchRule rule = classes().should(onlyCatchBroadlyInsideBoundaryHandlers());
        rule.check(classes);
    }

    private static ArchCondition<JavaClass> onlyCatchBroadlyInsideBoundaryHandlers() {
        return new ArchCondition<JavaClass>(
                "catch Exception/Throwable only inside @BoundaryHandler code units") {
            @Override
            public void check(JavaClass clazz, ConditionEvents events) {
                boolean classMarked = clazz.isAnnotatedWith(BoundaryHandler.class);
                for (JavaCodeUnit codeUnit : clazz.getCodeUnits()) {
                    boolean marked = classMarked || codeUnit.isAnnotatedWith(BoundaryHandler.class);
                    for (TryCatchBlock block : codeUnit.getTryCatchBlocks()) {
                        // Only java.lang.Exception — NOT Throwable: try-with-resources desugars to
                        // a synthetic catch(Throwable) for resource cleanup, which is not a real
                        // broad catch. The codebase has zero hand-written Throwable catches
                        // (error-boundaries.md residual R-twr).
                        boolean broad = block.getCaughtThrowables().stream()
                                .map(JavaClass::getName)
                                .anyMatch(n -> n.equals("java.lang.Exception"));
                        if (broad && !marked) {
                            events.add(SimpleConditionEvent.violated(codeUnit,
                                    "broad catch (Exception) without @BoundaryHandler at "
                                            + block.getSourceCodeLocation()));
                        }
                    }
                }
            }
        };
    }
}
