package com.azure.logstash.input.unit;

import com.azure.logstash.input.HealthState;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for HealthState — verifies state machine transitions based on poll results.
 */
public class HealthStateTest {

    // -----------------------------------------------------------------------
    // 1. Initial state is STARTING
    // -----------------------------------------------------------------------
    @Test
    public void testInitialStateIsStarting() {
        HealthState health = new HealthState();
        assertEquals(HealthState.State.STARTING, health.getState());
    }

    // -----------------------------------------------------------------------
    // 2. HEALTHY after successful poll (all succeed, none fail)
    // -----------------------------------------------------------------------
    @Test
    public void testHealthyAfterSuccessfulPoll() {
        HealthState health = new HealthState();
        health.recordPollResult(3, 0);
        assertEquals(HealthState.State.HEALTHY, health.getState());
    }

    // -----------------------------------------------------------------------
    // 3. DEGRADED after mixed poll (some succeed, some fail)
    // -----------------------------------------------------------------------
    @Test
    public void testDegradedAfterMixedPoll() {
        HealthState health = new HealthState();
        health.recordPollResult(2, 1);
        assertEquals(HealthState.State.DEGRADED, health.getState());
    }

    // -----------------------------------------------------------------------
    // 4. UNHEALTHY after consecutive all-fail cycles (default threshold = 3)
    // -----------------------------------------------------------------------
    @Test
    public void testUnhealthyAfterConsecutiveFailures() {
        HealthState health = new HealthState();
        // First all-fail cycle: DEGRADED
        health.recordPollResult(0, 5);
        assertEquals(HealthState.State.DEGRADED, health.getState());
        // Second all-fail cycle: still DEGRADED
        health.recordPollResult(0, 5);
        assertEquals(HealthState.State.DEGRADED, health.getState());
        // Third all-fail cycle: now UNHEALTHY (threshold reached)
        health.recordPollResult(0, 5);
        assertEquals(HealthState.State.UNHEALTHY, health.getState());
    }

    // -----------------------------------------------------------------------
    // 5. Recovery from UNHEALTHY to HEALTHY on successful poll
    // -----------------------------------------------------------------------
    @Test
    public void testRecoveryFromUnhealthy() {
        HealthState health = new HealthState();
        // Drive to UNHEALTHY
        health.recordPollResult(0, 5);
        health.recordPollResult(0, 5);
        health.recordPollResult(0, 5);
        assertEquals(HealthState.State.UNHEALTHY, health.getState());
        // Recover with a successful poll
        health.recordPollResult(1, 0);
        assertEquals(HealthState.State.HEALTHY, health.getState());
    }
}
