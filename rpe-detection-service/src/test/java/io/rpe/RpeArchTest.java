package io.rpe;

import io.rpe.config.BoundaryHandler;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.TryCatchBlock;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noMethods;

/**
 * ArchUnit rules enforcing critical coding constraints from Architecture Spec.
 *
 * These rules catch silent failure modes at compile/test time:
 *  - subscribe() with no error handler swallows exceptions silently
 *  - @Transactional on reactive chain causes invisible transaction loss after thread hop
 *  - @KafkaListener returning Mono/Flux is silently ignored by Spring Kafka
 */
class RpeArchTest {

    private static JavaClasses classes;

    @BeforeAll
    static void importClasses() {
        classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.rpe");
    }

    /**
     * All subscribe() calls must provide at minimum an onError handler.
     * subscribe() with no args swallows ALL exceptions silently — a violation produces
     * invisible missing alerts with no log, no metric, no observable failure.
     *
     * Rule: noClasses call subscribe() with zero arguments on Mono or Flux.
     */
    @Test
    void subscribeCallsMustHaveErrorHandler() {
        ArchRule rule = noClasses()
                .should()
                .callMethod(reactor.core.publisher.Mono.class, "subscribe")
                .orShould()
                .callMethod(reactor.core.publisher.Flux.class, "subscribe");

        rule.check(classes);
    }

    /**
     * @KafkaListener methods must return void — not merely "not Mono/Flux".
     * Spring Kafka silently ignores ANY non-void return value — the publisher is never
     * subscribed, meaning no Redis gate, no detection, no alerts — a complete silent failure.
     */
    @Test
    void kafkaListenerMethodsMustReturnVoid() {
        ArchRule rule = com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods()
                .that().areAnnotatedWith(org.springframework.kafka.annotation.KafkaListener.class)
                .should()
                .haveRawReturnType("void");

        rule.check(classes);
    }

    /**
     * No synchronized methods anywhere in RPE — synchronized pins virtual-thread carriers
     * and silently degrades ALL VT concurrency across the JVM. Code that must lock uses
     * ReentrantLock (reactive-pipeline.md Virtual Thread Rules).
     */
    @Test
    void noSynchronizedMethods() {
        ArchRule rule = noMethods()
                .should()
                .haveModifier(com.tngtech.archunit.core.domain.JavaModifier.SYNCHRONIZED);

        rule.check(classes);
    }

    /**
     * RandomUUID must not be used for alert_id generation.
     * alert_id = UUIDv5(NAMESPACE, eventId + ":" + ruleName) — deterministic and immutable.
     * Random UUIDs silently and permanently break processed_alerts dedup and replay safety (ADR-13).
     *
     * All alert_id generation must go through UuidV5.generate().
     */
    @Test
    void alertIdMustNotUseRandomUuid() {
        // Check that UuidV5 is used, not UUID.randomUUID(), in the domain/detection/consumer packages
        ArchRule rule = noClasses()
                .that().resideInAnyPackage(
                        "io.rpe.consumer..",
                        "io.rpe.detection..",
                        "io.rpe.domain..")
                .should()
                .callMethod(java.util.UUID.class, "randomUUID");

        rule.check(classes);
    }

    /**
     * ADR-17 §7 Stage 4 — no cross-service package dependency. Services communicate only over
     * Kafka (async) and share contracts as schemas, never as code (microservices.md §1.4/§2).
     * A compile-time dependency on a sibling service's package = a shared-code recoupling =
     * distributed monolith. This is a tripwire: it activates the instant someone adds a Maven
     * dependency on another service and imports its classes.
     */
    @Test
    void noCrossServicePackageDependencies() {
        ArchRule rule = noClasses()
                .should()
                .dependOnClassesThat()
                .resideInAnyPackage(
                        "io.rpe.relay..",
                        "io.rpe.alert..",
                        "io.rpe.triage..");

        rule.check(classes);
    }

    /**
     * ADR-17 §7 Stage 4 / non-goal #1 — the Detector set is the indivisible detection core and
     * lives ONLY in io.rpe.detection. This guard fails the moment a Detector is moved
     * toward a shared/util package or a would-be-extracted module — the first structural step of
     * splitting the atomic Lua gate from a rule (ADR-17 §6.2). New rules are new Detectors HERE,
     * never new services.
     */
    @Test
    void detectorImplementationsResideOnlyInDetectionPackage() {
        ArchRule rule = classes()
                .that().implement(io.rpe.detection.Detector.class)
                .should().resideInAPackage("io.rpe.detection");

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
