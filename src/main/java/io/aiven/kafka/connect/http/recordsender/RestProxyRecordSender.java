/*
 * Copyright 2019 Aiven Oy and http-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.http.recordsender;

import io.aiven.kafka.connect.http.sender.HttpSender;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RestProxyRecordSender extends RecordSender {

    private static final Logger log = LoggerFactory.getLogger(RestProxyRecordSender.class);

    private final int batchMaxSize;

    protected RestProxyRecordSender(final HttpSender httpSender,
            final int batchMaxSize) {
        super(httpSender);
        this.batchMaxSize = batchMaxSize;
    }

    @Override
    public void send(final Collection<SinkRecord> records) {
        final List<SinkRecord> batch = new ArrayList<>(batchMaxSize);
        for (final var record : records) {
            batch.add(record);
            if (batch.size() >= batchMaxSize) {
                final String body = createRequestBody(batch);
                batch.clear();
                httpSender.send(body);
            }
        }

        if (!batch.isEmpty()) {
            final String body = createRequestBody(batch);
            httpSender.send(body);
        }
    }

    @Override
    public void send(final SinkRecord record) {
        throw new ConnectException("Don't call this method for batch sending");
    }

    private String createRequestBody(final Collection<SinkRecord> batch) {
        final StringBuilder builder = new StringBuilder();
        builder.append("{\"records\": [");
        final Iterator<SinkRecord> it = batch.iterator();
        if (it.hasNext()) {
            builder.append(buildStringRecord(it.next()));
            while (it.hasNext()) {
                builder.append(",");
                builder.append(buildStringRecord(it.next()));
            }
        }

        builder.append("]}");
        log.info(builder.toString());
        return builder.toString();
    }

    private String buildStringRecord(final SinkRecord record) {
        final StringBuilder builder = new StringBuilder();
        builder.append("{\"key\": \"");
        if (record.key() instanceof byte[]) {
            builder.append(new String((byte[]) record.key(), StandardCharsets.UTF_8));
        } else {
            builder.append(record.key() != null ? (String) record.key() : "");
        }
        builder.append("\",\"value\": \"");
        if (record.value() instanceof byte[]) {
            builder.append(Base64.getEncoder().encodeToString((byte[]) record.value()));
        } else {
            builder.append((String) record.value());
        }
        builder.append("\"}");
        return builder.toString();
    }
}
