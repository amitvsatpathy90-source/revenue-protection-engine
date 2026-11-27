package io.rpe.triage.agent;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.rpe.triage.TriageTestSupport;
import io.rpe.triage.config.LlmResilienceConfig;
import io.rpe.triage.config.TriageProperties;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.model.tool.ToolCallingChatOptions;
import org.springframework.ai.tool.ToolCallback;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tool-round budget and verdict-call behavior of {@link TriageAgent} (ADR-31).
 * The tool call uses a mismatched accountId, so the guard answers without touching Redis.
 */
class TriageAgentVerdictRoundTest {

    private static final String MISMATCHED_ARGS = "{\"accountId\":\"someone-else\"}";

    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final ExecutorService vt = Executors.newVirtualThreadPerTaskExecutor();
    private final TriageProperties props = TriageTestSupport.lenientProps();

    @AfterEach
    void tearDown() {
        vt.close();
    }

    @Test
    void toolRoundBudgetOutsideRangeFailsStartup() {
        var model = new TriageTestSupport.StubChatModel(TriageTestSupport.VALID_VERDICT_JSON);

        assertThatThrownBy(() -> agent(model, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-tool-rounds");
        assertThatThrownBy(() -> agent(model, TriageAgent.MAX_TOOL_ROUNDS + 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> agent(model, 1)).doesNotThrowAnyException();
        assertThatCode(() -> agent(model, TriageAgent.MAX_TOOL_ROUNDS)).doesNotThrowAnyException();
    }

    @Test
    void verdictCallAfterLastToolRoundCarriesNoTools() {
        var model = new ScriptedModel(
                toolCallResponse(TriageTools.TOOL_ACCOUNT_HISTORY, MISMATCHED_ARGS),
                TriageTestSupport.cannedResponse(TriageTestSupport.VALID_VERDICT_JSON));

        var result = agent(model, 1).triage(TriageTestSupport.alert("velocity"));

        assertThat(result).isNotNull();
        assertThat(model.prompts).hasSize(2);
        assertThat(toolsOf(model.prompts.get(0))).isNotEmpty();   // first call exposes tools
        assertThat(toolsOf(model.prompts.get(1))).isEmpty();      // verdict call does not
    }

    @Test
    void toolCallOnVerdictCallDegradesWithRoundBudgetExhausted() {
        var model = new ScriptedModel(toolCallResponse(TriageTools.TOOL_ACCOUNT_HISTORY, MISMATCHED_ARGS));

        assertThatThrownBy(() -> agent(model, 1).triage(TriageTestSupport.alert("velocity")))
                .isInstanceOfSatisfying(TriageAgentException.class,
                        e -> assertThat(e.reason()).isEqualTo("round-budget-exhausted"));
        assertThat(model.prompts).hasSize(2);   // no third call
    }

    @Test
    void blankNarrativeIsSchemaValidationFailure() {
        var blank = "{\"severity\":\"LOW\",\"narrative\":\"\",\"evidence\":[],\"confidence\":0.4}";
        var model = new TriageTestSupport.StubChatModel(blank);

        assertThatThrownBy(() -> agent(model, 1).triage(TriageTestSupport.alert("velocity")))
                .isInstanceOfSatisfying(TriageAgentException.class,
                        e -> assertThat(e.reason()).isEqualTo("schema-validation"));
    }

    private TriageAgent agent(ChatModel model, int toolRounds) {
        var resilience = new LlmResilienceConfig().llmResilience(props, registry);
        var client = new TriageLlmClient(model, resilience, vt, registry, "test");
        return new TriageAgent(
                client,
                new EvidenceValidator(registry),
                new TriageTools(null, null, null, TriageTestSupport.objectMapper(), registry),
                TriageTestSupport.objectMapper(),
                props,
                registry,
                toolRounds);
    }

    private static List<ToolCallback> toolsOf(Prompt prompt) {
        assert prompt.getOptions() != null;
        var callbacks = ((ToolCallingChatOptions) prompt.getOptions()).getToolCallbacks();
        return callbacks == null ? List.of() : callbacks;
    }

    private static ChatResponse toolCallResponse(String tool, String args) {
        var message = AssistantMessage.builder()
                .content("")
                .toolCalls(List.of(new AssistantMessage.ToolCall("call-1", "function", tool, args)))
                .build();
        return new ChatResponse(List.of(new Generation(message)));
    }

    /**
     * Replays scripted responses in order; the last one repeats. Records every prompt.
     */
    private static final class ScriptedModel implements ChatModel {
        final List<Prompt> prompts = new ArrayList<>();
        private final Deque<ChatResponse> script;

        ScriptedModel(ChatResponse... responses) {
            this.script = new ArrayDeque<>(List.of(responses));
        }

        @Override
        public ChatResponse call(@NonNull Prompt prompt) {
            prompts.add(prompt);
            return script.size() > 1 ? script.removeFirst() : script.getFirst();
        }
    }
}

