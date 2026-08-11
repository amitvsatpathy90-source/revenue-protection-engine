package io.rpe.triage.service;

import io.rpe.triage.domain.TriagedAlertMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class KafkaTriagedVerdictPublisher implements TriagedVerdictPublisher {

    static final String TOPIC = "payment.alerts.triaged";

    private final KafkaTemplate<String, TriagedAlertMessage> kafkaTemplate;

    public KafkaTriagedVerdictPublisher(KafkaTemplate<String, TriagedAlertMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(TriagedAlertMessage verdict) {
        try {
            // Keyed by account for per-account ordering on the advisory topic.
            // Blocking get on a VT — parks, does not pin.
            kafkaTemplate.send(TOPIC, verdict.accountId(), verdict).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishFailedException("interrupted publishing triaged verdict", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new PublishFailedException("triaged verdict publish not confirmed", e);
        }
    }
}
