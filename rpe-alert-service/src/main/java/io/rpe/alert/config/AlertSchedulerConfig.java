package io.rpe.alert.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * JDBC scheduler for the alert service. The {@code processed_alerts} INSERT runs on this
 * dedicated BOUNDED platform-thread pool: PgJDBC {@code synchronized} internals pin VT
 * carrier threads, so JDBC must never run on a virtual-thread scheduler (immutable
 * constraint, reactive-pipeline.md). Extracted from the core {@code SchedulerConfig}.
 */
@Configuration
public class AlertSchedulerConfig {

    @Bean
    public Scheduler jdbcScheduler() {
        var pool = new ThreadPoolExecutor(
                10, 10, 0L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(500),
                r -> {
                    var t = new Thread(r);
                    t.setName("rpe-alert-jdbc-" + t.threadId());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        return Schedulers.fromExecutor(pool);
    }
}
