/*
 * Copyright (C) 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.OTHER_PERMANENT_ERRORS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.SKIPPED_EVENTS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.SUCCESSFUL_EVENTS_COUNTER_NAME;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.utils.WatchdogRunnable;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes Change events from DataStream into Cloud Spanner.
 *
 * <p>Change events are individually processed. Shadow tables store the version information(that
 * specifies the commit order) for each primary key. Shadow tables are consulted before actual
 * writes to Cloud Spanner to preserve the correctness and consistency of data.
 *
 * <p>Change events written successfully will be pushed onto the primary output with their commit
 * timestamps.
 *
 * <p>Change events that failed to be written will be pushed onto the secondary output tagged with
 * PERMANENT_ERROR_TAG/RETRYABLE_ERROR_TAG along with the exception that caused the failure.
 */
class SpannerWriterDoFn extends DoFn<String, Void> implements Serializable {

  // TODO - Change Cloud Spanner nomenclature in code used to read DDL.

  private static final Logger LOG = LoggerFactory.getLogger(SpannerWriterDoFn.class);

  private final SpannerConfig spannerConfig;

  /* SpannerAccessor must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor spannerAccessor;

  // Number of events successfully processed.
  private final Counter successfulEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, SUCCESSFUL_EVENTS_COUNTER_NAME);

  // Number of events skipped from being written to spanner because the events were stale.
  private final Counter skippedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, SKIPPED_EVENTS_COUNTER_NAME);

  private final Counter failedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, OTHER_PERMANENT_ERRORS_COUNTER_NAME);

  /*
   * The watchdog thread monitors the progress of Spanner transactions and ensures that they
   * are not stuck for an extended period of time. This is important because in load testing there were
   * instances where transactions were stuck, causing bottlenecks in the Dataflow pipeline.
   *
   * The WatchdogRunnable is designed to track if a transaction is making progress by comparing
   * the number of transaction attempts (`transactionAttemptCount`) over time. The `isInTransaction`
   * flag indicates whether a transaction is currently active. If the number of attempts remains
   * the same for a period of 15 minutes while the transaction is active, the watchdog logs a warning
   * and terminates the process by calling `System.exit(1)`.
   *
   * By running in the background, this watchdog thread ensures that long-running transactions
   * do not stall indefinitely, providing a safeguard mechanism for transaction processing in
   * the pipeline.
   */
  private transient AtomicLong transactionAttemptCount;
  private transient AtomicBoolean isInTransaction;
  private transient AtomicBoolean keepWatchdogRunning;
  private transient Thread watchdogThread;

  SpannerWriterDoFn(SpannerConfig spannerConfig) {
    Preconditions.checkNotNull(spannerConfig);
    this.spannerConfig = spannerConfig;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    // Setup and start the watchdog thread.
    transactionAttemptCount = new AtomicLong(0);
    isInTransaction = new AtomicBoolean(false);
    keepWatchdogRunning = new AtomicBoolean(true);
    watchdogThread =
        new Thread(
            new WatchdogRunnable(transactionAttemptCount, isInTransaction, keepWatchdogRunning),
            "SpannerWriterDoFn.WatchdogThread");
    watchdogThread.setDaemon(true);
    watchdogThread.start();
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() {
    spannerAccessor.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String msg = c.element();
    Gson gson = new Gson();
    JsonObject jsonMessage = gson.fromJson(msg, JsonObject.class);
    Mutation mutation = convertMessageToMutation(jsonMessage);
    /*
     * Try Catch block to capture any exceptions that might occur while processing
     * DataStream events while writing to Cloud Spanner. All Exceptions that are caught
     * can be retried based on the exception type.
     */
    try {
      // Start transaction
      spannerAccessor
          .getDatabaseClient()
          .readWriteTransaction()
          .run(
              (TransactionCallable<Void>)
                  transaction -> {
                    isInTransaction.set(true);
                    transactionAttemptCount.incrementAndGet();
                    Long id = jsonMessage.get("id").getAsLong();
                    com.google.cloud.spanner.Key shadowTableKey =
                        com.google.cloud.spanner.Key.of(id);
                    Struct shadowTableRow =
                        transaction.readRow(
                            "shadow_persons", shadowTableKey, List.of("last_write_timestamp"));
                    // If no row exists, treat as timestamp 0
                    long shadowTableTimestamp =
                        (shadowTableRow != null)
                            ? shadowTableRow.getLong("last_write_timestamp")
                            : 0L;
                    long messageTimestamp = jsonMessage.get("event_timestamp").getAsLong();

                    if (messageTimestamp <= shadowTableTimestamp) {
                      skippedEvents.inc();
                      return null;
                    }
                    transaction.buffer(mutation);
                    transaction.buffer(
                        Mutation.newInsertOrUpdateBuilder("shadow_persons")
                            .set("id")
                            .to(id)
                            .set("last_write_timestamp")
                            .to(Value.int64(messageTimestamp))
                            .build());
                    isInTransaction.set(false);
                    return null;
                  });
      successfulEvents.inc();
    } catch (Exception e) {
      LOG.error("failed for event: {}", msg, e);
      failedEvents.inc();
    }
  }

  private static Mutation convertMessageToMutation(JsonObject jsonMessage) {
    return Mutation.newInsertOrUpdateBuilder("persons")
        .set("id")
        .to(jsonMessage.get("id").getAsLong())
        .set("first_name1")
        .to(jsonMessage.get("first_name1").getAsString())
        .set("last_name1")
        .to(jsonMessage.get("last_name1").getAsString())
        .set("first_name2")
        .to(jsonMessage.get("first_name2").getAsString())
        .set("last_name2")
        .to(jsonMessage.get("last_name2").getAsString())
        .set("first_name3")
        .to(jsonMessage.get("first_name3").getAsString())
        .set("last_name3")
        .to(jsonMessage.get("last_name3").getAsString())
        .build();
  }
}
