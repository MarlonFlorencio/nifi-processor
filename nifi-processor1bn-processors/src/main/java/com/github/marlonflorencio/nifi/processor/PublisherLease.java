/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.marlonflorencio.nifi.processor;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.exception.TokenTooLargeException;
import org.apache.nifi.stream.io.util.StreamDemarcator;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class PublisherLease implements Closeable {
    private final ComponentLog logger;
    private final Producer<byte[], byte[]> producer;
    private final int maxMessageSize;
    private final long maxAckWaitMillis;
    private final boolean useTransactions;
    private final Pattern attributeNameRegex;
    private final Charset headerCharacterSet;
    private volatile boolean poisoned = false;
    private final AtomicLong messagesSent = new AtomicLong(0L);

    private volatile boolean transactionsInitialized = false;
    private volatile boolean activeTransaction = false;

    private InFlightMessageTracker tracker;

    public PublisherLease(final Producer<byte[], byte[]> producer, final int maxMessageSize, final long maxAckWaitMillis, final ComponentLog logger,
        final boolean useTransactions, final Pattern attributeNameRegex, final Charset headerCharacterSet) {
        this.producer = producer;
        this.maxMessageSize = maxMessageSize;
        this.logger = logger;
        this.maxAckWaitMillis = maxAckWaitMillis;
        this.useTransactions = useTransactions;
        this.attributeNameRegex = attributeNameRegex;
        this.headerCharacterSet = headerCharacterSet;
    }

    protected void poison() {
        this.poisoned = true;
    }

    public boolean isPoisoned() {
        return poisoned;
    }

    void beginTransaction() {
        if (!useTransactions) {
            return;
        }

        try {
            if (!transactionsInitialized) {
                producer.initTransactions();
                transactionsInitialized = true;
            }

            producer.beginTransaction();
            activeTransaction = true;
        } catch (final Exception e) {
            poison();
            throw e;
        }
    }

    void rollback() {
        if (!useTransactions || !activeTransaction) {
            return;
        }

        try {
            producer.abortTransaction();
        } catch (final Exception e) {
            poison();
            throw e;
        }

        activeTransaction = false;
    }

    void publish(final FlowFile flowFile, final InputStream flowFileContent, final byte[] messageKey, final byte[] demarcatorBytes, final String topic, final Integer partition) throws IOException {
        if (tracker == null) {
            tracker = new InFlightMessageTracker(logger);
        }

        try {
            byte[] messageContent;
            if (demarcatorBytes == null || demarcatorBytes.length == 0) {
                if (flowFile.getSize() > maxMessageSize) {
                    tracker.fail(flowFile, new TokenTooLargeException("A message in the stream exceeds the maximum allowed message size of " + maxMessageSize + " bytes."));
                    return;
                }
                // Send FlowFile content as it is, to support sending 0 byte message.
                messageContent = new byte[(int) flowFile.getSize()];
                StreamUtils.fillBuffer(flowFileContent, messageContent);
                publish(flowFile, messageKey, messageContent, topic, tracker, partition);
                return;
            }

            try (final StreamDemarcator demarcator = new StreamDemarcator(flowFileContent, demarcatorBytes, maxMessageSize)) {
                while ((messageContent = demarcator.nextToken()) != null) {
                    publish(flowFile, messageKey, messageContent, topic, tracker, partition);

                    if (tracker.isFailed(flowFile)) {
                        // If we have a failure, don't try to send anything else.
                        return;
                    }
                }
            } catch (final TokenTooLargeException ttle) {
                tracker.fail(flowFile, ttle);
            }
        } catch (final Exception e) {
            tracker.fail(flowFile, e);
            poison();
            throw e;
        }
    }

    private void addHeaders(final FlowFile flowFile, final Map<String, String> additionalAttributes, final ProducerRecord<?, ?> record) {
        if (attributeNameRegex == null) {
            return;
        }

        final Headers headers = record.headers();
        for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
            if (attributeNameRegex.matcher(entry.getKey()).matches()) {
                headers.add(entry.getKey(), entry.getValue().getBytes(headerCharacterSet));
            }
        }

        for (final Map.Entry<String, String> entry : additionalAttributes.entrySet()) {
            if (attributeNameRegex.matcher(entry.getKey()).matches()) {
                headers.add(entry.getKey(), entry.getValue().getBytes(headerCharacterSet));
            }
        }
    }

    protected void publish(final FlowFile flowFile, final byte[] messageKey, final byte[] messageContent, final String topic, final InFlightMessageTracker tracker, final Integer partition) {
        publish(flowFile, Collections.emptyMap(), messageKey, messageContent, topic, tracker, partition);
    }

    protected void publish(final FlowFile flowFile, final Map<String, String> additionalAttributes, final byte[] messageKey, final byte[] messageContent,
                           final String topic, final InFlightMessageTracker tracker, final Integer partition) {

        final Integer moddedPartition = partition == null ? null : Math.abs(partition) % (producer.partitionsFor(topic).size());
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, moddedPartition, messageKey, messageContent);
        addHeaders(flowFile, additionalAttributes, record);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                tracker.incrementAcknowledgedCount(flowFile);
            } else {
                tracker.fail(flowFile, exception);
                poison();
            }
        });

        messagesSent.incrementAndGet();
        tracker.incrementSentCount(flowFile);
    }


    public PublishResult complete() {
        if (tracker == null) {
            if (messagesSent.get() == 0L) {
                return PublishResult.EMPTY;
            }

            rollback();
            throw new IllegalStateException("Cannot complete publishing to Kafka because Publisher Lease was already closed");
        }

        try {
            producer.flush();

            if (activeTransaction) {
                producer.commitTransaction();
                activeTransaction = false;
            }
        } catch (final Exception e) {
            poison();
            throw e;
        }

        try {
            tracker.awaitCompletion(maxAckWaitMillis);
            return tracker.createPublishResult();
        } catch (final InterruptedException e) {
            logger.warn("Interrupted while waiting for an acknowledgement from Kafka; some FlowFiles may be transferred to 'failure' even though they were received by Kafka");
            Thread.currentThread().interrupt();
            return tracker.failOutstanding(e);
        } catch (final TimeoutException e) {
            logger.warn("Timed out while waiting for an acknowledgement from Kafka; some FlowFiles may be transferred to 'failure' even though they were received by Kafka");
            return tracker.failOutstanding(e);
        } finally {
            tracker = null;
        }
    }

    @Override
    public void close() {
        producer.close( Duration.ofMillis(TimeUnit.MILLISECONDS.toMillis(maxAckWaitMillis)));
        tracker = null;
    }

    public InFlightMessageTracker getTracker() {
        if (tracker == null) {
            tracker = new InFlightMessageTracker(logger);
        }

        return tracker;
    }
}
