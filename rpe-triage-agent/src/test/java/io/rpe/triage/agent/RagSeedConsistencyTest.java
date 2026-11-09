package io.rpe.triage.agent;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR-30 D3: every rule the agent knows needs a seed row under the same rule_name.
 */
class RagSeedConsistencyTest {

    private static final String SEED = "/db/migration/V3__seed_triage_rag_corpus.sql";
    // Row heads: "VALUES ('rule'," or "       ('rule',".
    private static final Pattern ROW_HEAD = Pattern.compile("(?m)(?:VALUES\\s*|^\\s+)\\('([a-z_]+)',");

    @Test
    void seededRuleNamesEqualToolMapKeys() throws IOException {
        String sql;
        try (var in = getClass().getResourceAsStream(SEED)) {
            assertThat(in).as("seed migration on classpath").isNotNull();
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        Set<String> seeded = new TreeSet<>();
        var m = ROW_HEAD.matcher(sql);
        while (m.find()) seeded.add(m.group(1));

        // Guards against the regex silently matching nothing
        assertThat(seeded).isNotEmpty();

        assertThat(seeded).isEqualTo(new TreeSet<>(TriageAgent.TOOLS_BY_RULE.keySet()));
    }
}
