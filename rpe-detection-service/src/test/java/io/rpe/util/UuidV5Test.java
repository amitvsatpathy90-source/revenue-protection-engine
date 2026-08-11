package io.rpe.util;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * UUIDv5 determinism is the keystone of RPE replay safety.
 * Same event + rule must always produce the same alert_id, forever.
 * These tests pin the expected output — any regression is a correctness break.
 */
class UuidV5Test {

    @Test
    void sameInputProducesSameUuid() {
        String name = "evt-123:velocity";
        UUID first  = UuidV5.generate(UuidV5.RPE_NAMESPACE, name);
        UUID second = UuidV5.generate(UuidV5.RPE_NAMESPACE, name);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void differentInputsProduceDifferentUuids() {
        UUID u1 = UuidV5.generate(UuidV5.RPE_NAMESPACE, "evt-1:velocity");
        UUID u2 = UuidV5.generate(UuidV5.RPE_NAMESPACE, "evt-1:zscore");
        UUID u3 = UuidV5.generate(UuidV5.RPE_NAMESPACE, "evt-2:velocity");

        assertThat(u1).isNotEqualTo(u2);
        assertThat(u1).isNotEqualTo(u3);
        assertThat(u2).isNotEqualTo(u3);
    }

    @Test
    void generatedUuidIsVersion5() {
        UUID u = UuidV5.generate(UuidV5.RPE_NAMESPACE, "test");
        assertThat(u.version()).isEqualTo(5);
    }

    @Test
    void generatedUuidIsRfc4122Variant() {
        UUID u = UuidV5.generate(UuidV5.RPE_NAMESPACE, "test");
        assertThat(u.variant()).isEqualTo(2); // RFC 4122
    }

    @Test
    void namespaceIsFixed() {
        // The RPE_NAMESPACE must never change — changing it invalidates all stored alert_ids.
        assertThat(UuidV5.RPE_NAMESPACE.toString())
                .isEqualTo("a6e4c1b8-4e2d-5f3a-9b0c-1d2e3f4a5b6c");
    }
}
