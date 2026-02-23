package com.azure.logstash.input;

import com.azure.logstash.input.tracking.RegistryStateTracker;
import com.azure.storage.blob.BlobContainerClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class MultiPrefixIT extends AzuriteTestBase {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String containerName;
    private BlobContainerClient containerClient;
    private List<Map<String, Object>> events;
    private String dbPath;

    @Before
    public void setUp() throws IOException {
        containerName = uniqueContainerName("multiprefix");
        containerClient = createContainer(containerName);
        events = new CopyOnWriteArrayList<>();
        dbPath = new File(tempFolder.getRoot(), "multiprefix.db").getAbsolutePath();
    }

    private BlobPoller createPoller(List<String> prefixes, List<String> excludePrefixes, int batchSize) {
        RegistryStateTracker stateTracker = new RegistryStateTracker(dbPath, "test-processor");
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);
        return new BlobPoller(containerClient, stateTracker, processor,
                events::add, prefixes, excludePrefixes, batchSize);
    }

    @Test
    public void testMultiplePrefixesOnlyProcessMatchingBlobs() {
        uploadBlob(containerName, "a/one.log", "alpha\n");
        uploadBlob(containerName, "b/two.log", "bravo\n");
        uploadBlob(containerName, "c/three.log", "charlie\n");

        BlobPoller poller = createPoller(Arrays.asList("a/", "b/"), Collections.emptyList(), 50);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals(2, summary.getBlobsProcessed());
        assertEquals(2, events.size());
        assertEquals("alpha", events.get(0).get("message"));
        assertEquals("bravo", events.get(1).get("message"));
    }

    @Test
    public void testExcludePrefixesSkipMatchingBlobs() {
        uploadBlob(containerName, "logs/app.log", "app-line\n");
        uploadBlob(containerName, "logs/debug/trace.log", "debug-line\n");
        uploadBlob(containerName, "logs/info.log", "info-line\n");

        BlobPoller poller = createPoller(Collections.singletonList("logs/"),
                Collections.singletonList("logs/debug/"), 50);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals(2, summary.getBlobsProcessed());
        assertEquals(1, summary.getBlobsExcluded());
        assertEquals(2, events.size());
        assertTrue(events.stream().anyMatch(e -> "app-line".equals(e.get("message"))));
        assertTrue(events.stream().anyMatch(e -> "info-line".equals(e.get("message"))));
        assertFalse(events.stream().anyMatch(e -> "debug-line".equals(e.get("message"))));
    }

    @Test
    public void testPrefixOrderingPreserved() {
        uploadBlob(containerName, "z/last.log", "zulu\n");
        uploadBlob(containerName, "a/first.log", "alpha\n");
        uploadBlob(containerName, "m/mid.log", "mike\n");

        BlobPoller poller = createPoller(Arrays.asList("z/", "a/", "m/"), Collections.emptyList(), 50);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals(3, summary.getBlobsProcessed());
        assertEquals(3, events.size());
        assertEquals("zulu", events.get(0).get("message"));
        assertEquals("alpha", events.get(1).get("message"));
        assertEquals("mike", events.get(2).get("message"));
    }

    @Test
    public void testEmptyPrefixesProcessesEntireContainer() {
        uploadBlob(containerName, "any/blob.log", "hello\n");
        uploadBlob(containerName, "other/blob.log", "world\n");

        BlobPoller poller = createPoller(Collections.emptyList(), Collections.emptyList(), 50);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals(2, summary.getBlobsProcessed());
        assertEquals(2, events.size());
    }
}
