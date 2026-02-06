package com.azure.logstash.input;

import com.azure.storage.blob.BlobContainerClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Operational tests for RBAC permissions against Azure Government.
 *
 * <p><b>Note:</b> Full RBAC tests (verifying that Data Reader cannot write tags,
 * Data Contributor cannot set tags, etc.) require creating additional service
 * principals with limited permissions. That setup is complex and out of scope
 * for the current test phase.
 *
 * <p>This class verifies that the current identity (which has Owner/Data Owner
 * permissions) can perform all required operations.
 */
@Category(OperationalTest.class)
public class RbacOT extends AzureGovTestBase {

    @Before
    public void setUp() {
        // Clean up incoming container (breaks leases first)
        cleanContainer("incoming");
        cleanContainer("archive");
    }

    // ── Test 1: Current identity has full access ────────────────────────────

    @Test
    public void testCurrentIdentityHasOwnerAccess() {
        // 1. List blobs (requires Data Reader)
        List<String> blobs = listBlobNames("incoming");
        assertNotNull("Should be able to list blobs", blobs);

        // 2. Upload blob (requires Data Contributor)
        uploadBlob("incoming", "rbac-test.log", "test data\n");
        assertEquals("test data\n", getBlobContent("incoming", "rbac-test.log"));

        // 3. Set tags (requires Data Owner)
        Map<String, String> tags = new HashMap<>();
        tags.put("logstash_status", "testing");
        tags.put("team", "ops");
        setBlobTags("incoming", "rbac-test.log", tags);

        Map<String, String> retrieved = getBlobTags("incoming", "rbac-test.log");
        assertEquals("testing", retrieved.get("logstash_status"));
        assertEquals("ops", retrieved.get("team"));

        // 4. Copy blob (requires Data Contributor on destination)
        // Copy from incoming to archive
        String sourceUrl = serviceClient.getBlobContainerClient("incoming")
                .getBlobClient("rbac-test.log").getBlobUrl();
        serviceClient.getBlobContainerClient("archive")
                .getBlobClient("rbac-test.log")
                .beginCopy(sourceUrl, null)
                .waitForCompletion();

        assertTrue("Blob should exist in archive",
                listBlobNames("archive").contains("rbac-test.log"));

        // 5. Delete blob (requires Data Contributor)
        serviceClient.getBlobContainerClient("incoming")
                .getBlobClient("rbac-test.log").delete();
        assertFalse("Blob should be deleted from incoming",
                listBlobNames("incoming").contains("rbac-test.log"));

        // Clean up archive
        serviceClient.getBlobContainerClient("archive")
                .getBlobClient("rbac-test.log").delete();
    }
}
