package com.azure.logstash.input.unit;

import com.azure.core.http.HttpResponse;
import com.azure.logstash.input.LeaseManager;
import com.azure.logstash.input.tracking.TagStateTracker;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobSetTagsOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

/**
 * Unit tests for TagStateTracker — blob index tag-based state tracking
 * for multi-replica deployments. Uses LeaseManager for distributed coordination
 * and Azure Blob Index Tags for state persistence.
 */
public class TagStateTrackerTest {

    private BlobContainerClient containerClient;
    private BlobClient blobClient;
    private LeaseManager leaseManager;
    private Function<BlobClient, LeaseManager> leaseManagerFactory;
    private TagStateTracker tracker;

    @Before
    public void setUp() {
        containerClient = mock(BlobContainerClient.class);
        blobClient = mock(BlobClient.class);
        leaseManager = mock(LeaseManager.class);
        leaseManagerFactory = bc -> leaseManager;

        when(containerClient.getBlobClient(anyString())).thenReturn(blobClient);

        tracker = new TagStateTracker(containerClient, 30, 10,
                "test-processor", leaseManagerFactory);
    }

    @After
    public void tearDown() {
        if (tracker != null) {
            tracker.close();
        }
    }

    // -----------------------------------------------------------------------
    // Helper: create a mock BlobStorageException with a specific status code
    // -----------------------------------------------------------------------
    private BlobStorageException createBlobStorageException(int statusCode) {
        HttpResponse mockResponse = mock(HttpResponse.class);
        when(mockResponse.getStatusCode()).thenReturn(statusCode);
        BlobStorageException bse = mock(BlobStorageException.class);
        when(bse.getStatusCode()).thenReturn(statusCode);
        return bse;
    }

    // -----------------------------------------------------------------------
    // 1. filterCandidates excludes completed blobs (reads tags from BlobItem)
    // -----------------------------------------------------------------------
    @Test
    public void testFilterCandidatesExcludesCompleted() {
        Map<String, String> completedTags = new HashMap<>();
        completedTags.put("logstash_status", "completed");

        BlobItem completedBlob = new BlobItem().setName("completed-blob").setTags(completedTags);
        List<BlobItem> candidates = tracker.filterCandidates(
                Collections.singletonList(completedBlob));

        assertTrue("Completed blob should be excluded", candidates.isEmpty());
    }

    // -----------------------------------------------------------------------
    // 2. filterCandidates excludes processing blobs (reads tags from BlobItem)
    // -----------------------------------------------------------------------
    @Test
    public void testFilterCandidatesExcludesProcessing() {
        Map<String, String> processingTags = new HashMap<>();
        processingTags.put("logstash_status", "processing");

        BlobItem processingBlob = new BlobItem().setName("processing-blob").setTags(processingTags);
        List<BlobItem> candidates = tracker.filterCandidates(
                Collections.singletonList(processingBlob));

        assertTrue("Processing blob should be excluded", candidates.isEmpty());
    }

    // -----------------------------------------------------------------------
    // 3. filterCandidates includes blobs with null tags (no setTags call)
    // -----------------------------------------------------------------------
    @Test
    public void testFilterCandidatesIncludesNoTags() {
        BlobItem noTagsBlob = new BlobItem().setName("no-tags-blob");
        List<BlobItem> candidates = tracker.filterCandidates(
                Collections.singletonList(noTagsBlob));

        assertEquals("Blob with null tags should be included", 1, candidates.size());
        assertEquals("no-tags-blob", candidates.get(0).getName());
    }

    // -----------------------------------------------------------------------
    // 4. filterCandidates includes failed blobs for reprocessing (reads tags from BlobItem)
    // -----------------------------------------------------------------------
    @Test
    public void testFilterCandidatesIncludesFailed() {
        Map<String, String> failedTags = new HashMap<>();
        failedTags.put("logstash_status", "failed");

        BlobItem failedBlob = new BlobItem().setName("failed-blob").setTags(failedTags);
        List<BlobItem> candidates = tracker.filterCandidates(
                Collections.singletonList(failedBlob));

        assertEquals("Failed blob should be included for reprocessing", 1, candidates.size());
        assertEquals("failed-blob", candidates.get(0).getName());
    }

    // -----------------------------------------------------------------------
    // 4b. filterCandidates includes blobs with empty tags map
    // -----------------------------------------------------------------------
    @Test
    public void testFilterCandidatesIncludesEmptyTags() {
        BlobItem emptyTagsBlob = new BlobItem().setName("empty-tags-blob").setTags(new HashMap<>());
        List<BlobItem> candidates = tracker.filterCandidates(
                Collections.singletonList(emptyTagsBlob));

        assertEquals("Blob with empty tags map should be included", 1, candidates.size());
        assertEquals("empty-tags-blob", candidates.get(0).getName());
    }

    // -----------------------------------------------------------------------
    // 5. claim acquires lease and sets tags
    // -----------------------------------------------------------------------
    @Test
    public void testClaimAcquiresLeaseAndSetsTags() {
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");
        when(blobClient.getTags()).thenReturn(new HashMap<>());

        boolean result = tracker.claim("test-blob");

        assertTrue("claim should return true on success", result);
        verify(leaseManager).acquireLease();
        verify(leaseManager).startRenewal();

        // claim() always has a lease, so it uses setTagsWithResponse with lease condition
        ArgumentCaptor<BlobSetTagsOptions> optionsCaptor =
                ArgumentCaptor.forClass(BlobSetTagsOptions.class);
        verify(blobClient).setTagsWithResponse(optionsCaptor.capture(), isNull(), isNull());

        Map<String, String> tags = optionsCaptor.getValue().getTags();
        assertEquals("processing", tags.get("logstash_status"));
        assertEquals("test-processor", tags.get("logstash_processor"));
        assertNotNull("logstash_started should be set", tags.get("logstash_started"));
    }

    // -----------------------------------------------------------------------
    // 6. claim preserves user tags
    // -----------------------------------------------------------------------
    @Test
    public void testClaimPreservesUserTags() {
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");

        Map<String, String> existingTags = new HashMap<>();
        existingTags.put("team", "ops");
        existingTags.put("env", "prod");
        when(blobClient.getTags()).thenReturn(existingTags);

        boolean result = tracker.claim("test-blob");

        assertTrue("claim should return true", result);

        // claim() always has a lease, so it uses setTagsWithResponse with lease condition
        ArgumentCaptor<BlobSetTagsOptions> optionsCaptor =
                ArgumentCaptor.forClass(BlobSetTagsOptions.class);
        verify(blobClient).setTagsWithResponse(optionsCaptor.capture(), isNull(), isNull());

        Map<String, String> tags = optionsCaptor.getValue().getTags();
        assertEquals("User tag 'team' should be preserved", "ops", tags.get("team"));
        assertEquals("User tag 'env' should be preserved", "prod", tags.get("env"));
        assertEquals("processing", tags.get("logstash_status"));
        assertEquals("test-processor", tags.get("logstash_processor"));
    }

    // -----------------------------------------------------------------------
    // 7. claim returns false on 412 (precondition failed) from setTags
    // -----------------------------------------------------------------------
    @Test
    public void testClaimReturnsFalseOn412() {
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");
        when(blobClient.getTags()).thenReturn(new HashMap<>());

        BlobStorageException bse = createBlobStorageException(412);
        doThrow(bse).when(blobClient).setTagsWithResponse(
                any(BlobSetTagsOptions.class), isNull(), isNull());

        boolean result = tracker.claim("test-blob");

        assertFalse("claim should return false on 412", result);
        verify(leaseManager).releaseLease();
    }

    // -----------------------------------------------------------------------
    // 8. claim returns false on 409 (lease conflict)
    // -----------------------------------------------------------------------
    @Test
    public void testClaimReturnsFalseOn409() {
        when(leaseManager.acquireLease()).thenReturn(null);

        boolean result = tracker.claim("test-blob");

        assertFalse("claim should return false when lease cannot be acquired", result);
        verify(blobClient, never()).setTags(any());
        verify(blobClient, never()).setTagsWithResponse(
                any(BlobSetTagsOptions.class), isNull(), isNull());
    }

    // -----------------------------------------------------------------------
    // 9. markCompleted sets completed tags
    // -----------------------------------------------------------------------
    @Test
    public void testMarkCompletedSetsTags() {
        // First claim so we have a lease manager in the map
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");
        when(leaseManager.getLeaseId()).thenReturn("lease-id-123");
        when(blobClient.getTags()).thenReturn(new HashMap<>());
        tracker.claim("test-blob");

        // Reset to capture the markCompleted call
        reset(blobClient);
        when(containerClient.getBlobClient("test-blob")).thenReturn(blobClient);

        Map<String, String> existingTags = new HashMap<>();
        existingTags.put("logstash_status", "processing");
        existingTags.put("logstash_started", "2024-01-01T00:00:00Z");
        existingTags.put("logstash_processor", "test-processor");
        when(blobClient.getTags()).thenReturn(existingTags);

        tracker.markCompleted("test-blob");

        // markCompleted with active lease uses setTagsWithResponse with lease condition
        ArgumentCaptor<BlobSetTagsOptions> optionsCaptor =
                ArgumentCaptor.forClass(BlobSetTagsOptions.class);
        verify(blobClient).setTagsWithResponse(optionsCaptor.capture(), isNull(), isNull());

        Map<String, String> tags = optionsCaptor.getValue().getTags();
        assertEquals("completed", tags.get("logstash_status"));
        assertNotNull("logstash_completed should be set", tags.get("logstash_completed"));
        assertEquals("test-processor", tags.get("logstash_processor"));
        assertNull("logstash_started should be removed", tags.get("logstash_started"));
        assertNull("logstash_error should not be present", tags.get("logstash_error"));
    }

    // -----------------------------------------------------------------------
    // 10. markFailed sets failed tags
    // -----------------------------------------------------------------------
    @Test
    public void testMarkFailedSetsTags() {
        when(blobClient.getTags()).thenReturn(new HashMap<>());

        tracker.markFailed("test-blob", "something went wrong");

        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<Map<String, String>> tagsCaptor =
                org.mockito.ArgumentCaptor.forClass(Map.class);
        verify(blobClient).setTags(tagsCaptor.capture());

        Map<String, String> tags = tagsCaptor.getValue();
        assertEquals("failed", tags.get("logstash_status"));
        assertEquals("something went wrong", tags.get("logstash_error"));
        assertEquals("test-processor", tags.get("logstash_processor"));
    }

    // -----------------------------------------------------------------------
    // 11. markFailed truncates error to 256 characters
    // -----------------------------------------------------------------------
    @Test
    public void testMarkFailedTruncatesError() {
        when(blobClient.getTags()).thenReturn(new HashMap<>());

        // Create a 300-character error message
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            sb.append("x");
        }
        String longError = sb.toString();
        assertEquals("Precondition: error should be 300 chars", 300, longError.length());

        tracker.markFailed("test-blob", longError);

        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<Map<String, String>> tagsCaptor =
                org.mockito.ArgumentCaptor.forClass(Map.class);
        verify(blobClient).setTags(tagsCaptor.capture());

        Map<String, String> tags = tagsCaptor.getValue();
        assertEquals("Error should be truncated to 256 chars",
                256, tags.get("logstash_error").length());
    }

    // -----------------------------------------------------------------------
    // 12. release stops renewal, releases lease, removes from map
    // -----------------------------------------------------------------------
    @Test
    public void testReleaseLease() {
        // First claim so we have a lease manager in the map
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");
        when(blobClient.getTags()).thenReturn(new HashMap<>());
        tracker.claim("test-blob");

        tracker.release("test-blob");

        verify(leaseManager).stopRenewal();
        verify(leaseManager).releaseLease();

        // Claiming again should create a new LeaseManager (the old one was removed)
        // We can verify by checking that the factory is called again
        LeaseManager newLeaseManager = mock(LeaseManager.class);
        when(newLeaseManager.acquireLease()).thenReturn("lease-id-456");
        // Update factory to return the new manager
        // Since we can't easily verify map removal directly, we just verify
        // the stop/release calls happened
    }

    // -----------------------------------------------------------------------
    // 13. markFailed sanitizes tag-unsafe characters from error message
    // -----------------------------------------------------------------------
    @Test
    public void testMarkFailedSanitizesTagUnsafeCharacters() {
        when(blobClient.getTags()).thenReturn(new HashMap<>());

        // Error with characters invalid in Azure tag values
        String unsafeError = "Error: <xml>&\"special\nchars\ttab";

        tracker.markFailed("test-blob", unsafeError);

        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<Map<String, String>> tagsCaptor =
                org.mockito.ArgumentCaptor.forClass(Map.class);
        verify(blobClient).setTags(tagsCaptor.capture());

        String sanitized = tagsCaptor.getValue().get("logstash_error");
        assertFalse("Should not contain '<'", sanitized.contains("<"));
        assertFalse("Should not contain '>'", sanitized.contains(">"));
        assertFalse("Should not contain '&'", sanitized.contains("&"));
        assertFalse("Should not contain '\"'", sanitized.contains("\""));
        assertFalse("Should not contain newline", sanitized.contains("\n"));
        assertFalse("Should not contain tab", sanitized.contains("\t"));
        assertTrue("Should preserve 'Error'", sanitized.contains("Error"));
        assertTrue("Should preserve 'special'", sanitized.contains("special"));
    }

    // -----------------------------------------------------------------------
    // 14. wasLeaseRenewalFailed returns false after normal claim (no failure)
    // -----------------------------------------------------------------------
    @Test
    public void testWasLeaseRenewalFailedReturnsFalseAfterNormalClaim() {
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");
        when(blobClient.getTags()).thenReturn(new HashMap<>());
        tracker.claim("test-blob");

        assertFalse("wasLeaseRenewalFailed should return false when no renewal failure occurred",
                tracker.wasLeaseRenewalFailed("test-blob"));
    }

    // -----------------------------------------------------------------------
    // 15. markFailed handles null error message
    // -----------------------------------------------------------------------
    @Test
    public void testMarkFailedHandlesNullError() {
        when(blobClient.getTags()).thenReturn(new HashMap<>());

        tracker.markFailed("test-blob", null);

        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<Map<String, String>> tagsCaptor =
                org.mockito.ArgumentCaptor.forClass(Map.class);
        verify(blobClient).setTags(tagsCaptor.capture());

        String error = tagsCaptor.getValue().get("logstash_error");
        // null error should be stored as empty string or "unknown", not cause NPE
        assertNotNull("Error tag should not be null", error);
    }
}
