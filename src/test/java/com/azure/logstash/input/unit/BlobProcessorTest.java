package com.azure.logstash.input.unit;

import com.azure.logstash.input.BlobProcessor;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobProperties;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for BlobProcessor — streams blob content line-by-line,
 * injects metadata, and produces events via a consumer callback.
 *
 * <p>Tests use a {@link TestableBlobProcessor} subclass that overrides
 * {@code openStream()} to supply a {@link ByteArrayInputStream}, bypassing
 * the Azure SDK's {@code BlobInputStream} return type which cannot be
 * trivially mocked via Mockito.
 */
public class BlobProcessorTest {

    private BlobClient blobClient;
    private BlobProperties blobProperties;
    private List<Map<String, Object>> capturedEvents;
    private Consumer<Map<String, Object>> eventConsumer;

    private static final String STORAGE_ACCOUNT = "testaccount";
    private static final String CONTAINER_NAME = "testcontainer";
    private static final String BLOB_NAME = "logs/test.log";
    private static final OffsetDateTime LAST_MODIFIED = OffsetDateTime.parse("2025-06-15T10:30:00Z");

    /**
     * Test subclass that overrides openStream() to return a ByteArrayInputStream
     * instead of calling blobClient.openInputStream() (which returns BlobInputStream).
     */
    private static class TestableBlobProcessor extends BlobProcessor {
        private final InputStream streamToReturn;

        TestableBlobProcessor(String storageAccount, String containerName,
                              boolean skipEmptyLines, InputStream stream) {
            super(storageAccount, containerName, skipEmptyLines);
            this.streamToReturn = stream;
        }

        @Override
        protected InputStream openStream(BlobClient blobClient) {
            return streamToReturn;
        }
    }

    @Before
    public void setUp() {
        blobClient = mock(BlobClient.class);
        blobProperties = mock(BlobProperties.class);

        when(blobClient.getBlobName()).thenReturn(BLOB_NAME);
        when(blobClient.getProperties()).thenReturn(blobProperties);
        when(blobProperties.getLastModified()).thenReturn(LAST_MODIFIED);

        capturedEvents = new ArrayList<>();
        eventConsumer = capturedEvents::add;
    }

    /**
     * Helper: create a TestableBlobProcessor with a ByteArrayInputStream
     * containing the given content.
     */
    private BlobProcessor createProcessor(String content, boolean skipEmptyLines) {
        ByteArrayInputStream stream = new ByteArrayInputStream(
                content.getBytes(StandardCharsets.UTF_8));
        return new TestableBlobProcessor(STORAGE_ACCOUNT, CONTAINER_NAME, skipEmptyLines, stream);
    }

    /**
     * Helper: create a TestableBlobProcessor with a custom InputStream.
     */
    private BlobProcessor createProcessorWithStream(InputStream stream, boolean skipEmptyLines) {
        return new TestableBlobProcessor(STORAGE_ACCOUNT, CONTAINER_NAME, skipEmptyLines, stream);
    }

    // -----------------------------------------------------------------------
    // 1. testProcessSimpleBlob — 3 lines -> 3 events with correct messages
    // -----------------------------------------------------------------------
    @Test
    public void testProcessSimpleBlob() throws IOException {
        BlobProcessor processor = createProcessor("line1\nline2\nline3", true);

        BlobProcessor.ProcessResult result = processor.process(blobClient, eventConsumer, () -> false);

        assertEquals(3, capturedEvents.size());
        assertEquals("line1", capturedEvents.get(0).get("message"));
        assertEquals("line2", capturedEvents.get(1).get("message"));
        assertEquals("line3", capturedEvents.get(2).get("message"));
        assertEquals(3, result.getEventCount());
        assertTrue(result.isCompleted());
    }

    // -----------------------------------------------------------------------
    // 2. testMetadataInjected — verify all 5 @metadata fields
    // -----------------------------------------------------------------------
    @Test
    public void testMetadataInjected() throws IOException {
        BlobProcessor processor = createProcessor("hello world", true);

        processor.process(blobClient, eventConsumer, () -> false);

        assertEquals(1, capturedEvents.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) capturedEvents.get(0).get("@metadata");
        assertNotNull("@metadata should be present", metadata);
        assertEquals(BLOB_NAME, metadata.get("azure_blob_name"));
        assertEquals(CONTAINER_NAME, metadata.get("azure_blob_container"));
        assertEquals(STORAGE_ACCOUNT, metadata.get("azure_blob_storage_account"));
        assertEquals(1L, metadata.get("azure_blob_line_number"));
        assertEquals(LAST_MODIFIED.toString(), metadata.get("azure_blob_last_modified"));
    }

    // -----------------------------------------------------------------------
    // 3. testLineNumbersCorrect — 5 lines -> line numbers 1-5
    // -----------------------------------------------------------------------
    @Test
    public void testLineNumbersCorrect() throws IOException {
        BlobProcessor processor = createProcessor("a\nb\nc\nd\ne", true);

        processor.process(blobClient, eventConsumer, () -> false);

        assertEquals(5, capturedEvents.size());
        for (int i = 0; i < 5; i++) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) capturedEvents.get(i).get("@metadata");
            assertEquals("Line number for event " + i, (long) (i + 1), metadata.get("azure_blob_line_number"));
        }
    }

    // -----------------------------------------------------------------------
    // 4. testSkipEmptyLinesEnabled — "a\n\nb\n\nc" -> 3 events, numbers 1,2,3
    // -----------------------------------------------------------------------
    @Test
    public void testSkipEmptyLinesEnabled() throws IOException {
        BlobProcessor processor = createProcessor("a\n\nb\n\nc", true);

        processor.process(blobClient, eventConsumer, () -> false);

        assertEquals(3, capturedEvents.size());
        assertEquals("a", capturedEvents.get(0).get("message"));
        assertEquals("b", capturedEvents.get(1).get("message"));
        assertEquals("c", capturedEvents.get(2).get("message"));

        // Line numbers should be sequential (1, 2, 3) — blanks don't count
        for (int i = 0; i < 3; i++) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) capturedEvents.get(i).get("@metadata");
            assertEquals((long) (i + 1), metadata.get("azure_blob_line_number"));
        }
    }

    // -----------------------------------------------------------------------
    // 5. testSkipEmptyLinesDisabled — blank lines produce events with empty message
    // -----------------------------------------------------------------------
    @Test
    public void testSkipEmptyLinesDisabled() throws IOException {
        BlobProcessor processor = createProcessor("a\n\nb\n\nc", false);

        processor.process(blobClient, eventConsumer, () -> false);

        assertEquals(5, capturedEvents.size());
        assertEquals("a", capturedEvents.get(0).get("message"));
        assertEquals("", capturedEvents.get(1).get("message"));
        assertEquals("b", capturedEvents.get(2).get("message"));
        assertEquals("", capturedEvents.get(3).get("message"));
        assertEquals("c", capturedEvents.get(4).get("message"));
    }

    // -----------------------------------------------------------------------
    // 6. testLargeLineNotTruncated — 10KB line delivered as-is
    // -----------------------------------------------------------------------
    @Test
    public void testLargeLineNotTruncated() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10240; i++) {
            sb.append('X');
        }
        String largeLine = sb.toString();
        BlobProcessor processor = createProcessor(largeLine, true);

        processor.process(blobClient, eventConsumer, () -> false);

        assertEquals(1, capturedEvents.size());
        assertEquals(largeLine, capturedEvents.get(0).get("message"));
        assertEquals(10240, ((String) capturedEvents.get(0).get("message")).length());
    }

    // -----------------------------------------------------------------------
    // 7. testEmptyBlob — zero bytes -> 0 events, completed=true
    // -----------------------------------------------------------------------
    @Test
    public void testEmptyBlob() throws IOException {
        BlobProcessor processor = createProcessor("", true);

        BlobProcessor.ProcessResult result = processor.process(blobClient, eventConsumer, () -> false);

        assertEquals(0, capturedEvents.size());
        assertEquals(0, result.getEventCount());
        assertTrue(result.isCompleted());
    }

    // -----------------------------------------------------------------------
    // 8. testEncodingErrorMarksFailure — InputStream that throws IOException
    // -----------------------------------------------------------------------
    @Test(expected = IOException.class)
    public void testEncodingErrorMarksFailure() throws IOException {
        InputStream failingStream = mock(InputStream.class);
        when(failingStream.read(any(byte[].class), anyInt(), anyInt()))
                .thenThrow(new IOException("Simulated encoding error"));

        BlobProcessor processor = createProcessorWithStream(failingStream, true);
        processor.process(blobClient, eventConsumer, () -> false);
    }

    // -----------------------------------------------------------------------
    // 9. testStopCheckBetweenLines — isStopped=true after 2 lines of 5
    // -----------------------------------------------------------------------
    @Test
    public void testStopCheckBetweenLines() throws IOException {
        BlobProcessor processor = createProcessor("line1\nline2\nline3\nline4\nline5", true);

        AtomicInteger callCount = new AtomicInteger(0);
        // Return false for first 2 calls, then true (stop after 2 events produced)
        BlobProcessor.ProcessResult result = processor.process(blobClient, eventConsumer, () -> {
            return callCount.incrementAndGet() > 2;
        });

        assertEquals(2, capturedEvents.size());
        assertEquals("line1", capturedEvents.get(0).get("message"));
        assertEquals("line2", capturedEvents.get(1).get("message"));
        assertFalse("Should not be completed when stopped early", result.isCompleted());
    }

    // -----------------------------------------------------------------------
    // 10. testEventCountReturned — verify ProcessResult.eventCount matches actual events
    // -----------------------------------------------------------------------
    @Test
    public void testEventCountReturned() throws IOException {
        BlobProcessor processor = createProcessor("one\ntwo\nthree\nfour", true);

        BlobProcessor.ProcessResult result = processor.process(blobClient, eventConsumer, () -> false);

        assertEquals("eventCount should match number of events emitted",
                capturedEvents.size(), result.getEventCount());
        assertEquals(4, result.getEventCount());
    }

    // -----------------------------------------------------------------------
    // 11. testProcessResultCompleted — successful processing -> completed=true
    // -----------------------------------------------------------------------
    @Test
    public void testProcessResultCompleted() throws IOException {
        BlobProcessor processor = createProcessor("first\nsecond\nthird", true);

        BlobProcessor.ProcessResult result = processor.process(blobClient, eventConsumer, () -> false);

        assertTrue("Successful processing should mark completed=true", result.isCompleted());
        assertEquals(3, result.getEventCount());
    }
}
