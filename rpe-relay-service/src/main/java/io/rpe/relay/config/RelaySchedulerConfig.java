package io.rpe.relay.config;

import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Scheduler beans for the relay service.
 *
 * Only the JDBC scheduler is needed here — the relay has no reactive Redis lane and no
 * WebFlux surface. All relay JDBC (outbox read, status transitions) runs on this
 * dedicated BOUNDED platform-thread pool: PgJDBC {@code synchronized} internals pin VT
 * carrier threads, so JDBC must never run on a virtual-thread scheduler (immutable
 * constraint, reactive-pipeline.md). Extracted from the core {@code SchedulerConfig}.
 */
@Configuration
public class RelaySchedulerConfig {

    // Captured for clean shutdown at context close — runs after the relay loop has stopped in its
    // SmartLifecycle phase (ADR-22), so no in-flight batch is cut off mid-statement.
    private ExecutorService jdbcPool;

    @Bean
    public Scheduler jdbcScheduler() {
        var pool = new ThreadPoolExecutor(
                20, 20, 0L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(500),
                r -> {
                    var t = new Thread(r);
                    t.setName("rpe-relay-jdbc-" + t.threadId());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        this.jdbcPool = pool;
        return Schedulers.fromExecutor(pool);
    }

    @PreDestroy
    public void shutdownPool() {
        if (jdbcPool != null) {
            jdbcPool.shutdown();
        }
    }
}
