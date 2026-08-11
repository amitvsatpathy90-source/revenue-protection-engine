package io.rpe.triage.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class ExecutorConfig {

    /**
     * Dedicated BOUNDED platform pool for all JDBC (inbox + processed_alerts tool).
     * PgJDBC {@code synchronized} pins VT carriers — same JVM constraint as the core
     * module (ai-triage-rules.md §2). Triage volume is ~1% of events; 4 threads is ample.
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService jdbcExecutor() {
        return new ThreadPoolExecutor(
                4, 4, 0L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100),
                r -> {
                    var t = new Thread(r, "triage-jdbc");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * VT-per-task executor for the TimeLimiter's futures. The blocking ChatClient
     * HTTP call parks the VT; {@code cancelRunningFuture=true} interrupts it on the
     * 12s cap so abandoned calls don't accumulate silently (ADR-15 §3.9).
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService llmExecutor() {
        return Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("triage-llm-", 0).factory());
    }
}
