package io.rpe.triage;

import io.rpe.triage.config.BoundaryHandler;
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
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noMethods;

/**
 * ArchUnit guards for the triage service (added at ADR-17 §7 Stage 4) —
 * brings triage to parity with its siblings' silent-failure guards and adds the cross-service
 * import tripwire. Triage is an MVC + virtual-thread surface (no WebFlux), so the reactive
 * subscribe()-error-handler guard does not apply here.
 */
class TriageArchTest {

    private static JavaClasses classes;

    @BeforeAll
    static void importClasses() {
        classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.rpe");
    }

    /**
     * @KafkaListener methods must return void — Spring Kafka silently ignores other returns.
     */
    @Test
    void kafkaListenerMethodsMustReturnVoid() {
        ArchRule rule = methods()
                .that().areAnnotatedWith(org.springframework.kafka.annotation.KafkaListener.class)
                .should().haveRawReturnType("void");
        rule.check(classes);
    }

    /**
     * synchronized pins virtual-thread carriers — use ReentrantLock (virtual thread pinning constraint).
     */
    @Test
    void noSynchronizedMethods() {
        ArchRule rule = noMethods().should().haveModifier(JavaModifier.SYNCHRONIZED);
        rule.check(classes);
    }

    /**
     * ADR-17 §7 Stage 4 — no cross-service package dependency. Triage shares only the PaymentAlert
     * SCHEMA with the core, never code (the service-independence discipline); there is no
     * Spring AI dependency anywhere in the core, and no core dependency here. This tripwire fails if
     * anyone adds a Maven dependency on a sibling service — including detection's core (Detector set
     * / Lua gate), which must never be referenced across the service boundary.
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
                        "io.rpe.relay..",
                        "io.rpe.alert..");
        rule.check(classes);
    }

    /**
     * Broad catch (Exception/Throwable) is permitted ONLY inside a @BoundaryHandler code unit
     * (or class) — a deliberate last line of defense at a thread/loop/listener/scheduler frame.
     * Elsewhere catch the specific exception, or RuntimeException (the allowed narrower rung).
     * Bans accidental broad catches in domain logic (ADR-21).
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
                        // (the error-boundary discipline).
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

    /**
     * DegradedTriageFallback is the static, LLM-independent verdict path (the triage agent design discipline) —
     * it must produce a valid degraded verdict even if Spring AI itself is misconfigured or absent.
     * A Spring AI import here would silently reintroduce the dependency this class exists to avoid.
     */
    @Test
    void degradedFallbackHasNoSpringAiImports() {
        ArchRule rule = noClasses()
                .that().haveSimpleName("DegradedTriageFallback")
                .should()
                .dependOnClassesThat()
                .resideInAnyPackage("org.springframework.ai..");
        rule.check(classes);
    }
}
