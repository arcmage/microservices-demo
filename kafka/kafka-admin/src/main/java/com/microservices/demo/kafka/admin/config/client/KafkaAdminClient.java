package com.microservices.demo.kafka.admin.config.client;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaAdminClient {

    private static  final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;

    private final RetryConfigData retryConfigData;

    private final AdminClient adminClient;

    private final RetryTemplate retryTemplate;

    public KafkaAdminClient(KafkaConfigData config,
                            RetryConfigData configData,
                            AdminClient client,
                            RetryTemplate template) {
        this.kafkaConfigData = config;
        this.retryConfigData = configData;
        this.adminClient = client;
        this.retryTemplate = template;
    }
}
