package io.rpe.triage.service;

/**
 * Indicates that publishing a triaged verdict was not confirmed by the broker.
 */
public class PublishFailedException extends RuntimeException {

    public PublishFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
