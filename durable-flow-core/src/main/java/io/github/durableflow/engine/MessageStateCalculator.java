package io.github.durableflow.engine;

import io.github.durableflow.api.MessageState;
import io.github.durableflow.api.StepState;
import io.github.durableflow.persistence.StepRecord;

import java.util.List;

/**
 * Derives the aggregate {@link MessageState} from the states of all steps.
 *
 * <p>Priority (highest to lowest):
 * <ol>
 *   <li>Any step FAILED_FINAL → PARKED</li>
 *   <li>Any step FAILED_RETRYABLE → ERROR</li>
 *   <li>Any step RUNNING or PENDING → IN_PROGRESS</li>
 *   <li>All steps SUCCEEDED (or SKIPPED) → PROCESSED</li>
 *   <li>No steps → RECEIVED</li>
 * </ol>
 */
public final class MessageStateCalculator {

    private MessageStateCalculator() {}

    public static MessageState calculate(List<StepRecord> steps) {
        if (steps == null || steps.isEmpty()) {
            return MessageState.RECEIVED;
        }

        boolean allDone = true;

        for (StepRecord step : steps) {
            StepState state = step.getStepState();
            if (state == StepState.FAILED_FINAL) {
                return MessageState.PARKED;
            }
            if (state == StepState.FAILED_RETRYABLE) {
                return MessageState.ERROR;
            }
            if (state == StepState.RUNNING || state == StepState.PENDING) {
                allDone = false;
            }
        }

        return allDone ? MessageState.PROCESSED : MessageState.IN_PROGRESS;
    }
}
