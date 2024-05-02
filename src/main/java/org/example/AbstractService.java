package org.example;

import org.springframework.retry.support.RetryTemplate;

public abstract class AbstractService {
    protected final RetryTemplate retryTemplate = RetryTemplate.builder()
        .exponentialBackoff(500, 2.0, 15000)
        .maxAttempts(7)
        .build();
}
