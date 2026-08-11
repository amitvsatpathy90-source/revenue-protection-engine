package io.rpe.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * UUID version 5 (SHA-1 name-based) generation.
 *
 * Used exclusively for deterministic alert_id derivation:
 *   alert_id = UUIDv5(RPE_NAMESPACE, eventId + ":" + ruleName)
 *
 * Same event + rule always produces the same alert_id, forever.
 * This is the keystone of replay safety: processed_alerts ON CONFLICT DO NOTHING
 * catches duplicate alert actions for the same event (ADR-13).
 *
 * DO NOT substitute with UUID.randomUUID() — random UUIDs silently and permanently
 * break replay safety.
 */
public final class UuidV5 {

    /** Fixed project namespace. Never change — changing invalidates all stored alert_ids. */
    public static final UUID RPE_NAMESPACE = UUID.fromString("a6e4c1b8-4e2d-5f3a-9b0c-1d2e3f4a5b6c");

    private UuidV5() {}

    public static UUID generate(UUID namespace, String name) {
        try {
            byte[] ns   = toBytes(namespace);
            byte[] nm   = name.getBytes(StandardCharsets.UTF_8);
            byte[] input = new byte[ns.length + nm.length];
            System.arraycopy(ns, 0, input, 0, ns.length);
            System.arraycopy(nm, 0, input, ns.length, nm.length);

            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] hash = sha1.digest(input);

            hash[6] = (byte) ((hash[6] & 0x0f) | 0x50); // version 5
            hash[8] = (byte) ((hash[8] & 0x3f) | 0x80); // RFC 4122 variant

            return fromBytes(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] toBytes(UUID uuid) {
        byte[] b   = new byte[16];
        long   msb = uuid.getMostSignificantBits();
        long   lsb = uuid.getLeastSignificantBits();
        for (int i = 7; i >= 0; i--) { b[i] = (byte) (msb & 0xff); msb >>= 8; }
        for (int i = 15; i >= 8; i--) { b[i] = (byte) (lsb & 0xff); lsb >>= 8; }
        return b;
    }

    private static UUID fromBytes(byte[] b) {
        long msb = 0, lsb = 0;
        for (int i = 0; i < 8;  i++) msb = (msb << 8) | (b[i]     & 0xff);
        for (int i = 8; i < 16; i++) lsb = (lsb << 8) | (b[i]     & 0xff);
        return new UUID(msb, lsb);
    }
}
