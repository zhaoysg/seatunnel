/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.enumerator;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.split.TiDBSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.utils.TableKeyRangeUtils;

import org.tikv.common.TiSession;
import org.tikv.kvproto.Coprocessor;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class TiDBSourceSplitEnumerator
        implements SourceSplitEnumerator<TiDBSourceSplit, TiDBSourceCheckpointState> {

    private final TiDBSourceConfig sourceConfig;
    private final Map<Integer, List<TiDBSourceSplit>> pendingSplit;
    private final Context<TiDBSourceSplit> context;

    private TiSession tiSession;
    private long tableId;

    private volatile boolean shouldEnumerate;

    private final Object stateLock = new Object();

    public TiDBSourceSplitEnumerator(
            @NonNull Context<TiDBSourceSplit> context,
            @NonNull TiDBSourceConfig sourceConfig) {
        this(context, sourceConfig, null);
    }

    public TiDBSourceSplitEnumerator(
            @NonNull Context<TiDBSourceSplit> context,
            @NonNull TiDBSourceConfig sourceConfig,
            TiDBSourceCheckpointState restoreState) {

        this.context = context;
        this.sourceConfig = sourceConfig;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = restoreState == null;

        if (restoreState != null) {
            this.shouldEnumerate = restoreState.isShouldEnumerate();
            this.pendingSplit.putAll(restoreState.getPendingSplit());
        }
    }

    @Override
    public void open() {
        this.tiSession = TiSession.create(sourceConfig.getTiConfiguration());

        this.tableId =
                this.tiSession
                        .getCatalog()
                        .getTable(
                                sourceConfig.getDatabaseName(),
                                sourceConfig.getTableName())
                        .getId();

        log.info(
                "TiDB source split enumerator opened, database={}, table={}, tableId={}, startupMode={}",
                sourceConfig.getDatabaseName(),
                sourceConfig.getTableName(),
                tableId,
                sourceConfig.getStartupMode());
    }

    /** The method is executed by the engine only once. */
    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();

        if (shouldEnumerate) {
            List<TiDBSourceSplit> sourceSplits = getTiDBSourceSplit();

            synchronized (stateLock) {
                addPendingSplit(sourceSplits);
                shouldEnumerate = false;
                assignSplit(readers);
            }
        }

        log.debug(
                "No more splits to assign. Sending NoMoreSplitsEvent to readers {}.",
                readers);

        readers.forEach(context::signalNoMoreSplits);
    }

    private synchronized void addPendingSplit(List<TiDBSourceSplit> splits) {
        splits.forEach(
                split ->
                        pendingSplit
                                .computeIfAbsent(
                                        getSplitOwner(
                                                split.splitId(),
                                                context.currentParallelism()),
                                        ignored -> new ArrayList<>())
                                .add(split));
    }

    private void assignSplit(Collection<Integer> readers) {
        for (Integer reader : readers) {
            List<TiDBSourceSplit> assignmentForReader = pendingSplit.remove(reader);

            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.debug(
                        "Assign splits {} to reader {}",
                        assignmentForReader,
                        reader);

                context.assignSplit(reader, assignmentForReader);
            }
        }
    }

    private static int getSplitOwner(String splitId, int numReaders) {
        return (splitId.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    private List<TiDBSourceSplit> getTiDBSourceSplit() {
        List<TiDBSourceSplit> sourceSplits = Lists.newArrayList();

        List<Coprocessor.KeyRange> keyRanges =
                TableKeyRangeUtils.getTableKeyRanges(
                        this.tableId,
                        context.currentParallelism());

        /*
         * All splits must use the same resolvedTs.
         *
         * INITIAL:
         *   Use -1 as the initial marker. After snapshot reading is completed,
         *   TiDBSourceReader will update resolvedTs to the snapshot TSO.
         *
         * EARLIEST:
         *   Start CDC from timestamp 0.
         *
         * LATEST:
         *   Obtain the current TiDB TSO once and start CDC from this timestamp,
         *   skipping historical data.
         */
        long resolvedTs = getStartupResolvedTs();

        log.info(
                "Create TiDB source splits, startupMode={}, resolvedTs={}, splitCount={}",
                sourceConfig.getStartupMode(),
                resolvedTs,
                keyRanges.size());

        for (Coprocessor.KeyRange keyRange : keyRanges) {
            sourceSplits.add(
                    new TiDBSourceSplit(
                            sourceConfig.getDatabaseName(),
                            sourceConfig.getTableName(),
                            keyRange,
                            resolvedTs,
                            keyRange.getStart(),
                            false));
        }

        return sourceSplits;
    }

    /**
     * Resolve the CDC starting timestamp according to startup mode.
     *
     * @return TiDB resolved timestamp
     */
    private long getStartupResolvedTs() {
        StartupMode startupMode = sourceConfig.getStartupMode();

        switch (startupMode) {
            case INITIAL:
                return -1L;

            case EARLIEST:
                return 0L;

            case LATEST:
                // Obtain the current TiDB TSO.
                // This value must only be obtained once for all splits.
                return tiSession.getTimestamp().getVersion();

            default:
                throw new IllegalArgumentException(
                        "Unsupported TiDB CDC startup mode: " + startupMode);
        }
    }

    /**
     * Called to close the enumerator, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    public void close() throws IOException {
        if (this.tiSession != null) {
            try {
                this.tiSession.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Add a split back to the split enumerator. It will only happen when a {@link
     * org.apache.seatunnel.api.source.SourceReader} fails and there are splits assigned to it after
     * the last successful checkpoint.
     *
     * @param splits The splits to add back for reassignment
     * @param subtaskId The ID of the subtask to which the returned splits belong
     */
    @Override
    public void addSplitsBack(List<TiDBSourceSplit> splits, int subtaskId) {
        log.debug(
                "Add back splits {} to TiDBSourceSplitEnumerator.",
                splits);

        if (!splits.isEmpty()) {
            addPendingSplit(splits);

            if (context.registeredReaders().contains(subtaskId)) {
                assignSplit(Collections.singletonList(subtaskId));
            } else {
                log.warn(
                        "Reader {} is not registered. Pending splits {} are not assigned.",
                        subtaskId,
                        splits);
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.values().stream().mapToInt(List::size).sum();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // Do nothing.
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug(
                "Register reader {} to TiDBSourceSplitEnumerator.",
                subtaskId);

        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    /**
     * If the source is bounded, checkpoint is not triggered.
     *
     * @param checkpointId checkpoint ID
     */
    @Override
    public TiDBSourceCheckpointState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new TiDBSourceCheckpointState(
                    shouldEnumerate,
                    pendingSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Do nothing.
    }
}
