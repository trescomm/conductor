/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.queue.sqs.config;

import com.amazonaws.services.sqs.AmazonSQS;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.contribs.queue.sqs.SQSObservableQueue.Builder;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import rx.Scheduler;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@Configuration
@EnableConfigurationProperties(SQSEventQueueProperties.class)
@ConditionalOnProperty(name = "conductor.event-queues.sqs.enabled", havingValue = "true")
public class SQSEventQueueConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQSEventQueueConfiguration.class);

    @Bean
    public EventQueueProvider sqsEventQueueProvider(Map<String, AmazonSQS> sqsClients, SQSEventQueueProperties properties,
                                                    Scheduler scheduler) {
        return new SQSEventQueueProvider(sqsClients, properties, scheduler);
    }


    /**
     *
     * @param conductorProperties app level properties
     * @param properties SQS queue properties
     * @param sqsClients map of clients, with key == region and value being the Amazon SQSClient
     * @return Map with key as region and value as map of status to queues
     */
    @ConditionalOnProperty(name = "conductor.default-event-queue.type", havingValue = "sqs", matchIfMissing = true)
    @Bean
    public Map<String, Map<Status, ObservableQueue>> getQueues(ConductorProperties conductorProperties,
                                                  SQSEventQueueProperties properties,
                                                  Map<String, AmazonSQS> sqsClients) {
        String stack = "";
        if (conductorProperties.getStack() != null && conductorProperties.getStack().length() > 0) {
            stack = conductorProperties.getStack() + "_";
        }
        Status[] statuses = new Status[]{Status.COMPLETED, Status.FAILED};
        Map<String, Map<Status, ObservableQueue>> queues = new HashMap<>();

        for(Map.Entry<String, AmazonSQS> entry : sqsClients.entrySet()) {
            String region = entry.getKey();
            AmazonSQS sqsClient = entry.getValue();

            Map<Status, ObservableQueue> queueMap = new HashMap<>();
            queues.put(region, queueMap);

            for (Status status : statuses) {
                String queuePrefix = StringUtils.isBlank(properties.getListenerQueuePrefix())
                        ? conductorProperties.getAppId() + "_sqs_notify_" + stack
                        : properties.getListenerQueuePrefix();

                String queueName = queuePrefix + status.name();
                LOGGER.info("Creating queue by name {} in region {}", queueName, region);

                Builder builder = new Builder().withClient(sqsClient).withQueueName(queueName);
                String auth = properties.getAuthorizedAccounts();
                //TODO: we should be able to configure authorized accounts per region

                String[] accounts = auth.split(",");
                for (String accountToAuthorize : accounts) {
                    accountToAuthorize = accountToAuthorize.trim();
                    if (accountToAuthorize.length() > 0) {
                        builder.addAccountToAuthorize(accountToAuthorize.trim());
                    }
                }
                ObservableQueue queue = builder.build();
                queueMap.put(status, queue);

            }

        }

        return queues;
    }
}
