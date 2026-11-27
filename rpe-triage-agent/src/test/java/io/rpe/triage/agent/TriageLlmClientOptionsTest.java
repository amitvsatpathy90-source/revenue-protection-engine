package io.rpe.triage.agent;

import io.rpe.triage.TriageTestSupport;
import io.rpe.triage.config.LlmResilienceConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.ChatOptions;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.model.tool.ToolCallingChatOptions;
import org.springframework.ai.openai.OpenAiChatOptions;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link TriageLlmClient#newToolCallingOptionsBuilder()}.
 *
 * <p>
 * Regression: a generic {@code ToolCallingChatOptions} made {@code OpenAiChatModel}
 * throw {@code ClassCastException}, degrading alerts with {@code reason=llm-error}.
 */
class TriageLlmClientOptionsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final ExecutorService executor =
            Executors.newVirtualThreadPerTaskExecutor();

    @AfterEach
    void tearDown() {
        executor.close();
    }

    /**
     * Verifies the builder keeps the provider options type and model defaults,
     * and that per-request overrides apply on top.
     */
    @Test
    void preservesProviderSpecificOptions() {
        OpenAiChatOptions modelDefaults = OpenAiChatOptions.builder()
                .model("llama3.2:3b")
                .baseUrl("http://127.0.0.1:11434/v1")
                .apiKey("test-key")
                .temperature(0.7)
                .build();

        ChatModel model = new ChatModel() {
            @Override
            public @NonNull ChatResponse call(@NonNull Prompt prompt) {
                throw new UnsupportedOperationException("Not used in this test");
            }

            @Override
            public @NonNull OpenAiChatOptions getOptions() {
                return modelDefaults;
            }
        };

        ToolCallingChatOptions options = client(model)
                .newToolCallingOptionsBuilder()
                .temperature(0.2)
                .toolContext(Map.of("accountId", "acc-test"))
                .build();

        assertThat(options).isInstanceOf(OpenAiChatOptions.class);

        OpenAiChatOptions actual = (OpenAiChatOptions) options;

        assertThat(actual.getModel()).isEqualTo("llama3.2:3b");
        assertThat(actual.getBaseUrl())
                .isEqualTo("http://127.0.0.1:11434/v1");
        assertThat(actual.getApiKey()).isEqualTo("test-key");
        assertThat(actual.getTemperature()).isEqualTo(0.2);
        assertThat(actual.getToolContext())
                .containsEntry("accountId", "acc-test");
        // builder must not mutate the shared model defaults
        assertThat(modelDefaults.getTemperature()).isEqualTo(0.7);
    }

    /**
     * Verifies a generic builder is returned when the model exposes no options.
     */
    @Test
    void fallsBackWhenModelDoesNotExposeOptions() {
        ChatModel model = new ChatModel() {
            @Override
            public @NonNull ChatResponse call(@NonNull Prompt prompt) {
                throw new UnsupportedOperationException("Not used in this test");
            }

            @Override
            public ChatOptions getOptions() {
                return null;
            }
        };

        ToolCallingChatOptions options = client(model)
                .newToolCallingOptionsBuilder()
                .build();

        assertThat(options).isNotNull()
                .isInstanceOf(ToolCallingChatOptions.class);
    }

    private TriageLlmClient client(ChatModel model) {
        var resilience = new LlmResilienceConfig()
                .llmResilience(TriageTestSupport.lenientProps(), registry);

        return new TriageLlmClient(
                model, resilience, executor, registry, "test");
    }
}
