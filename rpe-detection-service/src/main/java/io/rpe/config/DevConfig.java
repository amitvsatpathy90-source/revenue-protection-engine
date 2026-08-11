package io.rpe.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.tools.agent.ReactorDebugAgent;
import reactor.blockhound.BlockHound;

@Configuration
public class DevConfig {

    private static final Logger log = LoggerFactory.getLogger(DevConfig.class);

    /**
     * BlockHound detects blocking calls on threads that must not block (Netty I/O, Lettuce I/O).
     * 5–10% instrumentation overhead — dev/test only, never production.
     */
    @Bean
    @ConditionalOnProperty(name = "rpe.dev.blockhound.enabled", havingValue = "true")
    public Object blockHoundInstaller() {
        try {
            BlockHound.install();
            return "blockHoundInstalled";
        } catch (IllegalStateException ex) {
            log.warn("BlockHound could not be installed; continuing without it. Add -XX:+AllowRedefinitionToAddDeleteMethods to the JVM for full instrumentation.", ex);
            return "blockHoundSkipped";
        }
    }

    /**
     * Reactor Debug Agent captures assembly-time stack traces so reactive chain errors
     * show the full operator chain. Significant production overhead — dev/test only.
     */
    @Bean
    @ConditionalOnProperty(name = "rpe.dev.reactor-debug-agent", havingValue = "true")
    public Object reactorDebugAgent() {
        ReactorDebugAgent.init();
        return "reactorDebugAgentInitialized";
    }
}
