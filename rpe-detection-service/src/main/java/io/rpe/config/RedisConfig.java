package io.rpe.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Configuration
public class RedisConfig {

    /**
     * Unified Lua gate script — executed atomically per payment event.
     *
     * Returns an 8-element list; Spring Data Redis emits it as a single Flux element.
     * Mixed return types are handled by LuaGateResult.from(): integer RESP replies
     * map to Long, bulk-string replies map to String via StringRedisSerializer.
     */
    @Bean
    @SuppressWarnings("rawtypes")
    public DefaultRedisScript<List> gateScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>(loadLuaScript("lua/gate.lua"));
        script.setResultType(List.class);
        return script;
    }

    /**
     * Distributed rate-limiter token bucket (ADR-24). Separate from {@link #gateScript()}: it
     * runs BEFORE the gate so a throttled/breached event never mutates detection state, and the
     * gate's frozen step-order/return contract (ADR-14) is untouched.
     *
     * Returns a 2-element list [allowed (0|1), wait_ms]; both are Lua integer replies → Long.
     */
    @Bean
    @SuppressWarnings("rawtypes")
    public DefaultRedisScript<List> rateLimitScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>(loadLuaScript("lua/rate_limit.lua"));
        script.setResultType(List.class);
        return script;
    }

    /**
     * Loads a Lua script eagerly from the classpath so Redis {@code EVALSHA -> EVAL}
     * fallback never performs blocking resource I/O on a reactive execution path.
     *
     * @param path classpath location of the Lua script
     * @return script source loaded as UTF-8
     * @throws IllegalStateException if the script cannot be loaded
     */
    private String loadLuaScript(String path) {
        try {
            return new ClassPathResource(path)
                    .getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load Lua script: " + path, e);
        }
    }
}
