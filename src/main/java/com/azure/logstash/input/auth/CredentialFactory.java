package com.azure.logstash.input.auth;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

/**
 * Builds Azure credentials and a {@link BlobServiceClient} based on the configured
 * authentication method and cloud environment.
 *
 * <p>Supports three auth methods:
 * <ul>
 *   <li><b>default</b> — {@code DefaultAzureCredential} (workload identity, managed identity, CLI)</li>
 *   <li><b>connection_string</b> — Azure Storage connection string</li>
 *   <li><b>shared_key</b> — Storage account access key</li>
 * </ul>
 *
 * <p>Cloud auto-detection: if {@code cloud} is not explicitly set, the factory checks
 * the {@code AZURE_AUTHORITY_HOST} environment variable (set by AKS workload identity)
 * to determine the cloud environment.
 */
public class CredentialFactory {

    private static final Logger logger = LogManager.getLogger(CredentialFactory.class);

    private final String authMethod;
    private final String storageAccount;
    private final String connectionString;
    private final String storageKey;
    private final String blobEndpoint;
    private final String resolvedCloud;

    /**
     * Public constructor that uses {@link System#getenv} for environment variable lookups.
     *
     * @param authMethod       auth method: "default", "connection_string", or "shared_key"
     * @param storageAccount   Azure Storage account name
     * @param connectionString connection string (for connection_string auth)
     * @param storageKey       storage account key (for shared_key auth)
     * @param cloud            cloud environment name, or empty/"" for auto-detect
     * @param blobEndpoint     explicit blob endpoint override, or null/empty for default
     */
    public CredentialFactory(String authMethod, String storageAccount,
                             String connectionString, String storageKey,
                             String cloud, String blobEndpoint) {
        this(authMethod, storageAccount, connectionString, storageKey,
                cloud, blobEndpoint, System::getenv);
    }

    /**
     * Package-private constructor that accepts a custom environment variable supplier
     * for deterministic testing of cloud auto-detection.
     *
     * @param authMethod       auth method: "default", "connection_string", or "shared_key"
     * @param storageAccount   Azure Storage account name
     * @param connectionString connection string (for connection_string auth)
     * @param storageKey       storage account key (for shared_key auth)
     * @param cloud            cloud environment name, or empty/"" for auto-detect
     * @param blobEndpoint     explicit blob endpoint override, or null/empty for default
     * @param envVarSupplier   function to look up environment variables (replaces System.getenv)
     */
    // Visible for testing — allows deterministic env var injection
    public CredentialFactory(String authMethod, String storageAccount,
                      String connectionString, String storageKey,
                      String cloud, String blobEndpoint,
                      Function<String, String> envVarSupplier) {
        this.authMethod = authMethod;
        this.storageAccount = storageAccount;
        this.connectionString = connectionString;
        this.storageKey = storageKey;
        this.blobEndpoint = blobEndpoint;
        this.resolvedCloud = resolveCloud(cloud, envVarSupplier);
    }

    /**
     * Resolves the cloud environment.
     *
     * <p>If {@code cloud} is explicitly set (non-null, non-empty), it is used directly.
     * Otherwise, the {@code AZURE_AUTHORITY_HOST} environment variable is checked:
     * <ul>
     *   <li>Contains "microsoftonline.us" -- "AzureUSGovernment"</li>
     *   <li>Contains "chinacloudapi.cn" -- "AzureChinaCloud"</li>
     *   <li>Otherwise -- "AzureCloud"</li>
     * </ul>
     * Trailing slashes are stripped from the env var before matching.
     * If no env var is set either, defaults to "AzureCloud".
     */
    private String resolveCloud(String cloud, Function<String, String> envVarSupplier) {
        if (cloud != null && !cloud.isEmpty()) {
            return cloud;
        }

        String authorityHost = envVarSupplier.apply("AZURE_AUTHORITY_HOST");
        if (authorityHost != null && !authorityHost.isEmpty()) {
            // Strip trailing slash before matching
            String host = authorityHost.replaceAll("/$", "");

            String detected;
            if (host.contains("microsoftonline.us")) {
                detected = "AzureUSGovernment";
            } else if (host.contains("chinacloudapi.cn")) {
                detected = "AzureChinaCloud";
            } else {
                detected = "AzureCloud";
            }

            logger.info("Auto-detected cloud environment: {} (from AZURE_AUTHORITY_HOST={})",
                    detected, authorityHost);
            return detected;
        }

        return "AzureCloud";
    }

    /**
     * Returns the blob storage endpoint.
     *
     * <p>If {@code blobEndpoint} was explicitly set, returns it directly.
     * Otherwise constructs the endpoint from the resolved cloud and storage account:
     * <ul>
     *   <li>AzureCloud -- {@code https://{account}.blob.core.windows.net}</li>
     *   <li>AzureUSGovernment -- {@code https://{account}.blob.core.usgovcloudapi.net}</li>
     *   <li>AzureChinaCloud -- {@code https://{account}.blob.core.chinacloudapi.cn}</li>
     * </ul>
     *
     * @return the blob endpoint URL
     */
    public String getEndpoint() {
        if (blobEndpoint != null && !blobEndpoint.isEmpty()) {
            return blobEndpoint;
        }

        String suffix;
        switch (resolvedCloud) {
            case "AzureUSGovernment":
                suffix = "blob.core.usgovcloudapi.net";
                break;
            case "AzureChinaCloud":
                suffix = "blob.core.chinacloudapi.cn";
                break;
            case "AzureCloud":
            default:
                suffix = "blob.core.windows.net";
                break;
        }

        return "https://" + storageAccount + "." + suffix;
    }

    /**
     * Returns the Azure AD authority host URL for the resolved cloud.
     *
     * <ul>
     *   <li>AzureCloud -- {@code https://login.microsoftonline.com}</li>
     *   <li>AzureUSGovernment -- {@code https://login.microsoftonline.us}</li>
     *   <li>AzureChinaCloud -- {@code https://login.chinacloudapi.cn}</li>
     * </ul>
     *
     * @return the authority host URL
     */
    public String getAuthorityHost() {
        switch (resolvedCloud) {
            case "AzureUSGovernment":
                return "https://login.microsoftonline.us";
            case "AzureChinaCloud":
                return "https://login.chinacloudapi.cn";
            case "AzureCloud":
            default:
                return "https://login.microsoftonline.com";
        }
    }

    /**
     * Builds the appropriate credential object based on the auth method.
     *
     * <ul>
     *   <li>"default" -- returns a {@code DefaultAzureCredential}</li>
     *   <li>"connection_string" -- returns {@code null} (the BlobServiceClientBuilder
     *       uses the connection string directly)</li>
     *   <li>"shared_key" -- returns a {@code StorageSharedKeyCredential}</li>
     * </ul>
     *
     * @return the credential object, or null for connection_string auth
     */
    public Object buildCredential() {
        switch (authMethod) {
            case "default":
                return new DefaultAzureCredentialBuilder()
                        .authorityHost(getAuthorityHost())
                        .build();
            case "connection_string":
                return null;
            case "shared_key":
                return new StorageSharedKeyCredential(storageAccount, storageKey);
            default:
                throw new IllegalArgumentException("Unknown auth_method: " + authMethod);
        }
    }

    /**
     * Builds a {@link BlobServiceClient} using the configured auth method.
     *
     * <ul>
     *   <li>connection_string: builds with the connection string directly</li>
     *   <li>shared_key: builds with endpoint + StorageSharedKeyCredential</li>
     *   <li>default: builds with endpoint + DefaultAzureCredential</li>
     * </ul>
     *
     * @return a configured BlobServiceClient
     */
    public BlobServiceClient buildBlobServiceClient() {
        switch (authMethod) {
            case "connection_string":
                return new BlobServiceClientBuilder()
                        .connectionString(connectionString)
                        .buildClient();
            case "shared_key":
                return new BlobServiceClientBuilder()
                        .endpoint(getEndpoint())
                        .credential(new StorageSharedKeyCredential(storageAccount, storageKey))
                        .buildClient();
            case "default":
                return new BlobServiceClientBuilder()
                        .endpoint(getEndpoint())
                        .credential(new DefaultAzureCredentialBuilder()
                                .authorityHost(getAuthorityHost())
                                .build())
                        .buildClient();
            default:
                throw new IllegalArgumentException("Unknown auth_method: " + authMethod);
        }
    }

    /**
     * Returns the resolved cloud environment string.
     *
     * @return one of "AzureCloud", "AzureUSGovernment", or "AzureChinaCloud"
     */
    public String getResolvedCloud() {
        return resolvedCloud;
    }
}
