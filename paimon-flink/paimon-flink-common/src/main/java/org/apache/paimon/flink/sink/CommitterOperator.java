/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink;

import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Operator to commit {@link Committable}s for each snapshot. */
public class CommitterOperator extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<Committable, Committable>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** Record all the inputs until commit. */
    private final Deque<Committable> inputs = new ArrayDeque<>();

    /**
     * If checkpoint is enabled we should do nothing in {@link CommitterOperator#endInput}.
     * Remaining data will be committed in {@link CommitterOperator#notifyCheckpointComplete}. If
     * checkpoint is not enabled we need to commit remaining data in {@link
     * CommitterOperator#endInput}.
     */
    private final boolean streamingCheckpointEnabled;

    /**
     * This commitUser is valid only for new jobs. After the job starts, this commitUser will be
     * recorded into the states of write and commit operators. When the job restarts, commitUser
     * will be recovered from states and this value is ignored.
     */
    private final String initialCommitUser;

    /** Group the committable by the checkpoint id. */
    protected final NavigableMap<Long, ManifestCommittable> committablesPerCheckpoint;

    private final SerializableFunction<String, Committer> committerFactory;

    private final CommittableStateManager committableStateManager;

    /**
     * Aggregate committables to global committables and commit the global committables to the
     * external system.
     */
    protected Committer committer;

    private transient long currentWatermark;

    private transient boolean endInput;

    public CommitterOperator(
            boolean streamingCheckpointEnabled,
            String initialCommitUser,
            SerializableFunction<String, Committer> committerFactory,
            CommittableStateManager committableStateManager) {
        this.streamingCheckpointEnabled = streamingCheckpointEnabled;
        this.initialCommitUser = initialCommitUser;
        this.committablesPerCheckpoint = new TreeMap<>();
        this.committerFactory = checkNotNull(committerFactory);
        this.committableStateManager = committableStateManager;
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        this.currentWatermark = Long.MIN_VALUE;
        this.endInput = false;
        // each job can only have one user name and this name must be consistent across restarts
        // we cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint
        String commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);
        // parallelism of commit operator is always 1, so commitUser will never be null
        committer = committerFactory.apply(commitUser);

        committableStateManager.initializeState(context, committer);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        // Do not consume Long.MAX_VALUE watermark in case of batch or bounded stream
        if (mark.getTimestamp() != Long.MAX_VALUE) {
            this.currentWatermark = mark.getTimestamp();
        }
    }

    private ManifestCommittable toCommittables(long checkpoint, List<Committable> inputs)
            throws Exception {
        return committer.combine(checkpoint, currentWatermark, inputs);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        pollInputs();
        committableStateManager.snapshotState(context, committables(committablesPerCheckpoint));
    }

    private List<ManifestCommittable> committables(NavigableMap<Long, ManifestCommittable> map) {
        return new ArrayList<>(map.values());
    }

    @Override
    public void endInput() throws Exception {
        endInput = true;
        if (streamingCheckpointEnabled) {
            return;
        }

        pollInputs();
        commitUpToCheckpoint(Long.MAX_VALUE);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        commitUpToCheckpoint(endInput ? Long.MAX_VALUE : checkpointId);
    }

    private void commitUpToCheckpoint(long checkpointId) throws Exception {
        NavigableMap<Long, ManifestCommittable> headMap =
                committablesPerCheckpoint.headMap(checkpointId, true);
        committer.commit(committables(headMap));
        headMap.clear();
    }

    @Override
    public void processElement(StreamRecord<Committable> element) {
        output.collect(element);
        this.inputs.add(element.getValue());
    }

    @Override
    public void close() throws Exception {
        committablesPerCheckpoint.clear();
        inputs.clear();
        committer.close();
        super.close();
    }

    private void pollInputs() throws Exception {
        Map<Long, List<Committable>> grouped = new HashMap<>();
        for (Committable c : inputs) {
            grouped.computeIfAbsent(c.checkpointId(), k -> new ArrayList<>()).add(c);
        }

        for (Map.Entry<Long, List<Committable>> entry : grouped.entrySet()) {
            Long cp = entry.getKey();
            List<Committable> committables = entry.getValue();
            if (committablesPerCheckpoint.containsKey(cp)) {
                throw new RuntimeException(
                        String.format(
                                "Repeatedly commit the same checkpoint files. \n"
                                        + "The previous files is %s, \n"
                                        + "and the subsequent files is %s",
                                committablesPerCheckpoint.get(cp), committables));
            }

            committablesPerCheckpoint.put(cp, toCommittables(cp, committables));
        }

        this.inputs.clear();
    }
}
