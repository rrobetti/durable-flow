package io.github.durableflow;

import io.github.durableflow.api.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class RetryPolicyTest {

    @Test
    void fixedDelay_returnsConstantDelay() {
        RetryPolicy policy = RetryPolicy.fixedDelay(3, Duration.ofSeconds(2));
        assertEquals(Duration.ofSeconds(2), policy.nextDelay(1));
        assertEquals(Duration.ofSeconds(2), policy.nextDelay(2));
        assertEquals(Duration.ofSeconds(2), policy.nextDelay(3));
    }

    @Test
    void noRetry_shouldNotRetry() {
        RetryPolicy policy = RetryPolicy.noRetry();
        assertFalse(policy.shouldRetry(0, new RuntimeException()));
        assertFalse(policy.shouldRetry(1, new RuntimeException()));
    }

    @Test
    void defaultPolicy_allowsThreeAttempts() {
        RetryPolicy policy = RetryPolicy.defaultPolicy();
        assertEquals(3, policy.getMaxAttempts());
        assertTrue(policy.shouldRetry(0, new RuntimeException()));
        assertTrue(policy.shouldRetry(2, new RuntimeException()));
        assertFalse(policy.shouldRetry(3, new RuntimeException()));
    }

    @Test
    void exponentialBackoff_growsWithMultiplier() {
        RetryPolicy policy = RetryPolicy.exponentialBackoff(5,
                Duration.ofMillis(100), 2.0, Duration.ofSeconds(10), false);
        Duration d1 = policy.nextDelay(1);
        Duration d2 = policy.nextDelay(2);
        Duration d3 = policy.nextDelay(3);
        assertEquals(100, d1.toMillis());
        assertEquals(200, d2.toMillis());
        assertEquals(400, d3.toMillis());
    }

    @Test
    void exponentialBackoff_cappedAtMax() {
        RetryPolicy policy = RetryPolicy.exponentialBackoff(10,
                Duration.ofMillis(1000), 2.0, Duration.ofSeconds(5), false);
        // 1000 * 2^4 = 16000ms > 5000ms cap
        assertTrue(policy.nextDelay(5).toMillis() <= 5000);
    }

    @Test
    void exponentialBackoff_withJitter_withinBounds() {
        RetryPolicy policy = RetryPolicy.exponentialBackoff(5,
                Duration.ofMillis(1000), 2.0, Duration.ofSeconds(30), true);
        for (int i = 1; i <= 5; i++) {
            Duration d = policy.nextDelay(i);
            assertTrue(d.toMillis() >= 0, "Delay should be non-negative");
        }
    }

    @Test
    void shouldRetry_respectsExceptionClassifier() {
        RetryPolicy policy = RetryPolicy.fixedDelay(3, Duration.ofMillis(100));
        // Default classifier – all retryable
        assertTrue(policy.shouldRetry(0, new RuntimeException()));
        assertTrue(policy.shouldRetry(0, new IllegalStateException()));
    }

    @Test
    void negativeMaxAttempts_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> RetryPolicy.fixedDelay(-1, Duration.ofSeconds(1)));
    }
}
