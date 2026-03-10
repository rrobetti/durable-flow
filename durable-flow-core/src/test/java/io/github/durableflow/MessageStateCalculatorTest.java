package io.github.durableflow;

import io.github.durableflow.api.MessageState;
import io.github.durableflow.api.StepState;
import io.github.durableflow.engine.MessageStateCalculator;
import io.github.durableflow.persistence.StepRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageStateCalculatorTest {

    @Test
    void emptySteps_returnsReceived() {
        assertEquals(MessageState.RECEIVED, MessageStateCalculator.calculate(Collections.emptyList()));
    }

    @Test
    void nullSteps_returnsReceived() {
        assertEquals(MessageState.RECEIVED, MessageStateCalculator.calculate(null));
    }

    @Test
    void allSucceeded_returnsProcessed() {
        List<StepRecord> steps = List.of(
                stepWith(StepState.SUCCEEDED),
                stepWith(StepState.SUCCEEDED));
        assertEquals(MessageState.PROCESSED, MessageStateCalculator.calculate(steps));
    }

    @Test
    void anyFailedFinal_returnsParked() {
        List<StepRecord> steps = List.of(
                stepWith(StepState.SUCCEEDED),
                stepWith(StepState.FAILED_FINAL));
        assertEquals(MessageState.PARKED, MessageStateCalculator.calculate(steps));
    }

    @Test
    void anyFailedRetryable_returnsError() {
        List<StepRecord> steps = List.of(
                stepWith(StepState.SUCCEEDED),
                stepWith(StepState.FAILED_RETRYABLE));
        assertEquals(MessageState.ERROR, MessageStateCalculator.calculate(steps));
    }

    @Test
    void failedFinalTakesPriorityOverRetryable() {
        List<StepRecord> steps = List.of(
                stepWith(StepState.FAILED_FINAL),
                stepWith(StepState.FAILED_RETRYABLE));
        assertEquals(MessageState.PARKED, MessageStateCalculator.calculate(steps));
    }

    @Test
    void anyRunning_returnsInProgress() {
        List<StepRecord> steps = List.of(
                stepWith(StepState.SUCCEEDED),
                stepWith(StepState.RUNNING));
        assertEquals(MessageState.IN_PROGRESS, MessageStateCalculator.calculate(steps));
    }

    @Test
    void anyPending_returnsInProgress() {
        List<StepRecord> steps = List.of(
                stepWith(StepState.SUCCEEDED),
                stepWith(StepState.PENDING));
        assertEquals(MessageState.IN_PROGRESS, MessageStateCalculator.calculate(steps));
    }

    @Test
    void skippedCombinedWithSucceeded_returnsProcessed() {
        List<StepRecord> steps = List.of(
                stepWith(StepState.SUCCEEDED),
                stepWith(StepState.SKIPPED));
        assertEquals(MessageState.PROCESSED, MessageStateCalculator.calculate(steps));
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private StepRecord stepWith(StepState state) {
        StepRecord r = new StepRecord();
        r.setStepState(state);
        return r;
    }
}
