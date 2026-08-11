package io.rpe.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

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
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/gate.lua"));
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
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/rate_limit.lua"));
        script.setResultType(List.class);
        return script;
    }
}
