package com.azure.logstash.input;

import co.elastic.logstash.api.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

@LogstashPlugin(name = "azure_storage_blob")
public class AzureBlobStorageInput implements Input {
    private String id;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped;

    public AzureBlobStorageInput(String id, Configuration config, Context context) {
        this.id = id;
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        try { while (!stopped) { Thread.sleep(1000); } }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        finally { stopped = true; done.countDown(); }
    }

    @Override public void stop() { stopped = true; }
    @Override public void awaitStop() throws InterruptedException { done.await(); }
    @Override public Collection<PluginConfigSpec<?>> configSchema() { return Collections.emptyList(); }
    @Override public String getId() { return id; }
}
