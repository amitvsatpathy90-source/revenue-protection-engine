package io.rpe.triage.agent;

import io.rpe.triage.config.BoundaryHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Phase-1 tools (ADR-15 §3.4). ALL READ-ONLY — a tool needing a write is a design
 * error; stop and re-anchor (ai-triage-rules.md §1.3). Never writes to outbox,
 * processed_alerts, payment.alerts, or any core Redis key.
 *
 * <b>Account scoping (injection containment):</b> the model supplies {@code accountId}
 * as a tool argument, but free text in the alert could instruct it to query a
 * DIFFERENT account. Each call is therefore validated against the triage run's own
 * account (passed via {@link ToolContext}, which the model cannot influence); a
 * mismatch returns a bounded error string and increments
 * {@code triage.tool.account_mismatch} — a live injection-attempt signal.
 *
 * Responses are bounded (≤ {@value #MAX_EVENTS} items — rules §4): no unbounded
 * history dumps into the prompt.
 */
@Component
public class TriageTools {

    private static final Logger log = LoggerFactory.getLogger(TriageTools.class);

    public static final String TOOL_ACCOUNT_HISTORY = "redisAccountHistory";
    public static final String TOOL_RECENT_ALERTS   = "recentAlertsForAccount";
    public static final String CTX_ACCOUNT_ID       = "accountId";

    private static final String ACCOUNT_MISMATCH_ERROR =
            "{\"error\":\"account mismatch: tools may only query the alert's own account\"}";

    private static final int MAX_EVENTS = 20;

    private final StringRedisTemplate redis;
    private final DataSource          dataSource;
    private final ExecutorService     jdbcExecutor;
    private final ObjectMapper        objectMapper;
    private final MeterRegistry       meterRegistry;

    public TriageTools(
            StringRedisTemplate redis,
            DataSource dataSource,
            @Qualifier("jdbcExecutor") ExecutorService jdbcExecutor,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this.redis         = redis;
        this.dataSource    = dataSource;
        this.jdbcExecutor  = jdbcExecutor;
        this.objectMapper  = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @Tool(name = TOOL_ACCOUNT_HISTORY, description = """
            Read-only snapshot of the account's recent payment activity: rolling amount \
            statistics (event count, mean amount), last known location timestamp, and up \
            to 20 recent event timestamps from the velocity window.""")
    @BoundaryHandler("tool-boundary: any failure returns a bounded error to the model, never aborts the agent round (ai-triage-rules §3.4)")
    public String redisAccountHistory(
            @ToolParam(description = "The account id from the alert") String accountId,
            ToolContext toolContext) {
        String scoped = requireAlertAccount(accountId, toolContext, TOOL_ACCOUNT_HISTORY);
        if (scoped == null) {
            return ACCOUNT_MISMATCH_ERROR;
        }
        try {
            Map<String, Object> snapshot = new LinkedHashMap<>();
            Map<Object, Object> stats = redis.opsForHash().entries("stats:" + scoped);
            snapshot.put("stats", Map.of(
                    "eventCount", stats.getOrDefault("count", "0"),
                    "meanAmount", stats.getOrDefault("mean", "0")));

            Map<Object, Object> geo = redis.opsForHash().entries("geo:" + scoped);
            snapshot.put("lastLocationTimestampMs", geo.getOrDefault("ts", null));

            Set<String> recent = redis.opsForZSet().reverseRange("vel:" + scoped, 0, MAX_EVENTS - 1);
            snapshot.put("recentEventIdsInWindow", recent == null ? List.of() : recent);

            return objectMapper.writeValueAsString(snapshot);
        } catch (Exception e) {
            // Deterministic bounded error to the model — a tool exception must not
            // abort the round; the model can still produce a lower-confidence verdict
            log.warn("redisAccountHistory tool failed: {}", e.getMessage());
            return "{\"error\":\"account history temporarily unavailable\"}";
        }
    }

    @Tool(name = TOOL_RECENT_ALERTS, description = """
            Read-only list of up to 20 most recent fraud alerts previously raised for \
            the account: rule name and time for each.""")
    @BoundaryHandler("tool-boundary: any failure returns a bounded error to the model, never aborts the agent round (ai-triage-rules §3.4)")
    public String recentAlertsForAccount(
            @ToolParam(description = "The account id from the alert") String accountId,
            ToolContext toolContext) {
        String scoped = requireAlertAccount(accountId, toolContext, TOOL_RECENT_ALERTS);
        if (scoped == null) {
            return ACCOUNT_MISMATCH_ERROR;
        }
        try {
            List<Map<String, Object>> alerts = CompletableFuture.supplyAsync(
                    () -> queryRecentAlerts(scoped), jdbcExecutor).join();
            return objectMapper.writeValueAsString(Map.of("recentAlerts", alerts));
        } catch (Exception e) {
            log.warn("recentAlertsForAccount tool failed: {}", e.getMessage());
            return "{\"error\":\"alert history temporarily unavailable\"}";
        }
    }

    /** JDBC on the dedicated platform pool only — PgJDBC pins VT carriers. */
    private List<Map<String, Object>> queryRecentAlerts(String accountId) {
        String sql = "SELECT rule_name, produced_at FROM processed_alerts "
                   + "WHERE account_id = ? ORDER BY produced_at DESC LIMIT " + MAX_EVENTS;
        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(5);
            ps.setString(1, accountId);
            List<Map<String, Object>> rows = new ArrayList<>();
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(Map.of(
                            "ruleName", rs.getString("rule_name"),
                            "producedAt", rs.getTimestamp("produced_at").toInstant().toString()));
                }
            }
            return rows;
        } catch (SQLException e) {
            throw new IllegalStateException("processed_alerts query failed", e);
        }
    }

    /**
     * @return the alert's own account id, or {@code null} when the model asked for a
     *         different account (injection attempt or hallucinated argument).
     */
    private String requireAlertAccount(String requested, ToolContext toolContext, String tool) {
        Object expected = toolContext.getContext().get(CTX_ACCOUNT_ID);
        if (expected instanceof String exp && exp.equals(requested)) {
            return exp;
        }
        meterRegistry.counter("triage.tool.account_mismatch", "tool", tool).increment();
        log.warn("Tool {} requested non-alert account — denied (possible injection)", tool);
        return null;
    }
}
