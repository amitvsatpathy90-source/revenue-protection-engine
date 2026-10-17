package io.rpe.triage.rageval;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.ai.embedding.EmbeddingModel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Caches generated embeddings so repeated evaluation runs reuse the same vectors.
 *
 * <p>The cache key includes the embedding model identity and input content so vectors generated
 * by different models are not accidentally reused.
 *
 */
final class CachedEmbeddingClient {

    // Spring AI abstraction used to generate an embedding for the supplied text.
    private final EmbeddingModel model;

    // Identifies the embedding model used for this cache.
    private final String modelId;

    // File used to persist cached embeddings between runs.
    private final Path cacheFile;

    // In-memory cache: SHA-256 cache key -> embedding vector.
    private final Map<String, float[]> cache;

    // Jackson mapper used to load and persist the cache file.
    private final ObjectMapper mapper = new ObjectMapper();

    CachedEmbeddingClient(
            EmbeddingModel model,
            String modelId,
            Path cacheFile) throws IOException {

        this.model = model;
        this.modelId = modelId;
        this.cacheFile = cacheFile;

        // Load an existing cache so already-generated vectors are reused.
        this.cache = Files.exists(cacheFile)
                ? mapper.readValue(
                cacheFile.toFile(),
                new TypeReference<Map<String, float[]>>() {})
                : new LinkedHashMap<>();
    }

    float[] embed(String content) {

        // Reuse the cached vector when the same model/input combination was seen before.
        return cache.computeIfAbsent(
                hash(content),
                k -> model.embed(content));
    }

    void flush() throws IOException {

        // Persist the current in-memory cache to disk.
        mapper.writeValue(cacheFile.toFile(), cache);
    }

    private String hash(String content) {
        try {

            // Bind the vector to both the selected model and the exact input text.
            var digest = MessageDigest.getInstance("SHA-256")
                    .digest((modelId + ":" + content).getBytes(StandardCharsets.UTF_8));

            return HexFormat.of().formatHex(digest);

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
