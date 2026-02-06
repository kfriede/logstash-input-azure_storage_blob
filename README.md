# Logstash Input Plugin for Azure Blob Storage

A [Logstash](https://www.elastic.co/logstash) input plugin that reads blobs from [Azure Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/) and streams their content line-by-line into your Logstash pipeline.

Built with Microsoft's first-party Java Azure SDKs, this plugin runs natively on the JVM alongside Logstash - no Ruby shims or REST wrappers required. It supports Azure Commercial, Azure Government, and Azure China clouds out of the box.

## Features

- **Streaming architecture** - processes blobs line-by-line without loading entire files into memory
- **Multi-replica safe** - blob leases prevent duplicate processing across multiple Logstash instances
- **Three state-tracking strategies** - choose the right trade-off between permissions, portability, and coordination
- **Azure Government and sovereign clouds** - first-class support with automatic cloud detection
- **Flexible authentication** - DefaultAzureCredential (managed identity, workload identity, CLI), connection strings, or shared keys

## Installation

```bash
# From a local .gem file
/usr/share/logstash/bin/logstash-plugin install --no-verify logstash-input-azure_storage_blob-0.1.0-java.gem
```

Verify the plugin is installed:

```bash
/usr/share/logstash/bin/logstash-plugin list | grep azure_storage_blob
```

## Quick Start

The simplest configuration requires only a storage account and container name. Authentication defaults to `DefaultAzureCredential`, which automatically picks up managed identity, workload identity, Azure CLI credentials, or environment variables.

```ruby
input {
  azure_storage_blob {
    storage_account => "mystorageaccount"
    container       => "logs"
  }
}

output {
  stdout { codec => rubydebug }
}
```

## Configuration Reference

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `storage_account` | string | Azure Storage account name. |
| `container` | string | Name of the blob container to read from. |

### Authentication

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `auth_method` | string | `"default"` | Authentication method. One of `default`, `connection_string`, or `shared_key`. |
| `connection_string` | string | `""` | Azure Storage connection string. Required when `auth_method => "connection_string"`. |
| `storage_key` | string | `""` | Storage account access key. Required when `auth_method => "shared_key"`. |

**`auth_method` options:**

| Value | Description | When to use |
|-------|-------------|-------------|
| `default` | Uses `DefaultAzureCredential` - automatically tries managed identity, workload identity, environment variables, and Azure CLI in order. | Production deployments on Azure (AKS, VMs, App Service). |
| `connection_string` | Authenticates with a full connection string. | Quick setup, development, or environments without Azure AD. |
| `shared_key` | Authenticates with the storage account access key. | When you need key-based auth without a full connection string. |

### Azure Environment

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `cloud` | string | `""` (auto-detect) | Azure cloud environment. One of `AzureCloud`, `AzureUSGovernment`, or `AzureChinaCloud`. When empty, auto-detects from the `AZURE_AUTHORITY_HOST` environment variable. |
| `blob_endpoint` | string | `""` | Explicit blob service endpoint URL. Overrides the endpoint derived from `cloud` and `storage_account`. Use for private endpoints or the Azurite emulator. |

**Cloud endpoints:**

| `cloud` value | Storage endpoint | Auth endpoint |
|---------------|-----------------|---------------|
| `AzureCloud` | `*.blob.core.windows.net` | `login.microsoftonline.com` |
| `AzureUSGovernment` | `*.blob.core.usgovcloudapi.net` | `login.microsoftonline.us` |
| `AzureChinaCloud` | `*.blob.core.chinacloudapi.cn` | `login.chinacloudapi.cn` |

### State Tracking

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `tracking_strategy` | string | `"tags"` | How the plugin tracks which blobs have been processed. One of `tags`, `container`, or `registry`. |
| `archive_container` | string | `"archive"` | Container where completed blobs are moved. Only used with `container` strategy. |
| `error_container` | string | `"errors"` | Container where failed blobs are moved. Only used with `container` strategy. |
| `registry_path` | string | `"/data/registry.db"` | Path to the local SQLite database file. Only used with `registry` strategy. |

**`tracking_strategy` options:**

| Strategy | How it works | RBAC required | Multi-replica safe |
|----------|-------------|---------------|-------------------|
| `tags` | Writes blob index tags (`logstash_status`, etc.) to mark blobs as processing/completed/failed. Uses blob leases for coordination. | Storage Blob Data Owner | Yes |
| `container` | Moves completed blobs to an archive container and failed blobs to an error container. Uses blob leases for coordination. | Storage Blob Data Contributor | Yes |
| `registry` | Tracks state in a local SQLite database. No Azure-side state changes. | Storage Blob Data Reader | **No** - single instance only |

### Polling

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `poll_interval` | number | `30` | Seconds to wait between poll cycles. |
| `prefix` | string | `""` | Only process blobs whose name starts with this prefix. Empty means all blobs. |
| `blob_batch_size` | number | `10` | Maximum number of blobs to process per poll cycle. |

### Processing

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `skip_empty_lines` | boolean | `true` | Whether to skip empty lines when reading blob content. |

### Lease Coordination

These settings apply only to the `tags` and `container` tracking strategies.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `lease_duration` | number | `30` | Blob lease duration in seconds. Must be between 15 and 60 (Azure requirement). |
| `lease_renewal` | number | `20` | How often to renew the lease, in seconds. Should be less than `lease_duration`. |

## Event Metadata

Every event produced by this plugin includes the following `@metadata` fields:

| Field | Description |
|-------|-------------|
| `[@metadata][azure_blob_name]` | Name of the source blob. |
| `[@metadata][azure_blob_container]` | Name of the source container. |
| `[@metadata][azure_blob_storage_account]` | Storage account the blob was read from. |
| `[@metadata][azure_blob_line_number]` | Line number within the blob (1-based). |
| `[@metadata][azure_blob_last_modified]` | Last-modified timestamp of the blob (ISO 8601). |

You can use these in filters and outputs:

```ruby
filter {
  mutate {
    add_field => {
      "source_blob" => "%{[@metadata][azure_blob_name]}"
    }
  }
}
```

## Examples

### Ingest JSON logs from Azure Government

```ruby
input {
  azure_storage_blob {
    storage_account   => "govlogsstorage"
    container         => "application-logs"
    cloud             => "AzureUSGovernment"
    tracking_strategy => "tags"
    prefix            => "prod/"
    poll_interval     => 10
  }
}

filter {
  json { source => "message" }
}

output {
  elasticsearch {
    hosts => ["https://elasticsearch:9200"]
    index => "app-logs-%{+YYYY.MM.dd}"
  }
}
```

### Connection string auth with container-based tracking

```ruby
input {
  azure_storage_blob {
    storage_account    => "myaccount"
    container          => "incoming"
    auth_method        => "connection_string"
    connection_string  => "${AZURE_STORAGE_CONNECTION_STRING}"
    tracking_strategy  => "container"
    archive_container  => "processed"
    error_container    => "failed"
  }
}

output {
  stdout { codec => rubydebug }
}
```

Blobs are moved from `incoming` to `processed` on success, or to `failed` on error. Create all three containers before starting the pipeline.

### Lightweight single-instance setup with SQLite registry

```ruby
input {
  azure_storage_blob {
    storage_account   => "myaccount"
    container         => "data"
    auth_method       => "shared_key"
    storage_key       => "${AZURE_STORAGE_KEY}"
    tracking_strategy => "registry"
    registry_path     => "/var/lib/logstash/blob-registry.db"
    blob_batch_size   => 50
    poll_interval     => 60
  }
}

output {
  file {
    path => "/var/log/ingested/%{[@metadata][azure_blob_name]}.log"
  }
}
```

The `registry` strategy only requires `Storage Blob Data Reader` permissions and stores state locally. Do not run multiple Logstash instances with this strategy - they will process the same blobs independently.

### Multi-replica deployment on Kubernetes

```ruby
input {
  azure_storage_blob {
    storage_account   => "prodlogs"
    container         => "events"
    tracking_strategy => "tags"
    poll_interval     => 5
    blob_batch_size   => 20
    lease_duration    => 60
    lease_renewal     => 40
  }
}
```

With the `tags` or `container` strategy, you can safely run multiple Logstash replicas against the same container. Blob leases ensure each blob is processed by exactly one replica. On AKS with workload identity, `DefaultAzureCredential` picks up the pod identity automatically and the `cloud` setting is auto-detected from `AZURE_AUTHORITY_HOST`.

### Route events by blob name

```ruby
input {
  azure_storage_blob {
    storage_account => "myaccount"
    container       => "logs"
  }
}

output {
  if [@metadata][azure_blob_name] =~ /^access-logs/ {
    elasticsearch {
      index => "access-logs-%{+YYYY.MM.dd}"
    }
  } else if [@metadata][azure_blob_name] =~ /^error-logs/ {
    elasticsearch {
      index => "error-logs-%{+YYYY.MM.dd}"
    }
  }
}
```

## Building from Source

Requires Java 11+ and a local Logstash installation.

```bash
# Run unit tests
./gradlew test

# Run integration tests (requires Azurite)
docker run -d --name azurite -p 10000:10000 mcr.microsoft.com/azure-storage/azurite
./gradlew integrationTest

# Build the gem
./gradlew gem
```

## License

Apache-2.0
