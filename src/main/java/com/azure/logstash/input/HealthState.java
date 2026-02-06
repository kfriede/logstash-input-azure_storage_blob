package com.azure.logstash.input;

import java.util.LinkedList;

/**
 * Tracks the health state of the plugin based on poll cycle outcomes.
 *
 * <p>The state machine transitions are:</p>
 * <ul>
 *   <li>{@link State#STARTING} — initial state before any poll cycle completes</li>
 *   <li>{@link State#HEALTHY} — most recent cycle had only successes (or empty container on first poll)</li>
 *   <li>{@link State#DEGRADED} — mixed success/failure, or recent all-fail below the unhealthy threshold</li>
 *   <li>{@link State#UNHEALTHY} — consecutive all-fail cycles have reached the threshold</li>
 * </ul>
 *
 * <p>Any successful processing clears the failure streak and resets to HEALTHY.</p>
 */
public class HealthState {

    public enum State {
        STARTING,
        HEALTHY,
        DEGRADED,
        UNHEALTHY
    }

    private State currentState = State.STARTING;
    private final int unhealthyThreshold;
    private final LinkedList<Boolean> recentResults = new LinkedList<>();

    /**
     * Creates a HealthState with the default unhealthy threshold of 3 consecutive
     * all-fail cycles.
     */
    public HealthState() {
        this(3);
    }

    /**
     * Creates a HealthState with a custom unhealthy threshold.
     *
     * @param unhealthyThreshold number of consecutive all-fail cycles before
     *                           transitioning to UNHEALTHY
     */
    public HealthState(int unhealthyThreshold) {
        this.unhealthyThreshold = unhealthyThreshold;
    }

    /**
     * Records the outcome of a poll cycle and updates the health state.
     *
     * @param processed number of blobs successfully processed in this cycle
     * @param failed    number of blobs that failed in this cycle
     */
    public void recordPollResult(int processed, int failed) {
        boolean anySuccess = processed > 0;
        boolean anyFailure = failed > 0;

        if (anySuccess && !anyFailure) {
            // All succeeded
            currentState = State.HEALTHY;
        } else if (anySuccess && anyFailure) {
            // Mixed results
            currentState = State.DEGRADED;
        } else if (!anySuccess && anyFailure) {
            // All failed — track consecutive failures
            recentResults.addLast(false);
            if (recentResults.size() > unhealthyThreshold) {
                recentResults.removeFirst();
            }
            if (recentResults.size() >= unhealthyThreshold
                    && recentResults.stream().noneMatch(b -> b)) {
                currentState = State.UNHEALTHY;
            } else {
                currentState = State.DEGRADED;
            }
        } else {
            // Empty cycle (no blobs at all) — if still starting, transition to HEALTHY
            if (currentState == State.STARTING) {
                currentState = State.HEALTHY;
            }
            // Otherwise leave state unchanged
        }

        // On any success, clear the failure streak
        if (anySuccess) {
            recentResults.clear();
        }
    }

    /**
     * Returns the current health state.
     */
    public State getState() {
        return currentState;
    }
}
