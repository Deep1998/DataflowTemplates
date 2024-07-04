/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryTypeMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.RangePreparedStatementSetter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ReadWithUniformPartitions<T> extends PTransform<PBegin, PCollection<T>> {

  private static final Logger logger = LoggerFactory.getLogger(ReadWithUniformPartitions.class);

  /* We try to get Ranges, where a Range is split if it's count is greater than twice the mean */
  public static final long SPLITTER_MAX_RELATIVE_DEVIATION = 1;

  public static final long SPLITTER_DEFAULT_COUNT_QUERY_TIMEOUT_MILLIS = 30 * 1000;

  abstract SerializableFunction<Void, DataSource> dataSourceProviderFn();

  abstract UniformSplitterDBAdapter dbAdapter();

  abstract long countQueryTimeoutMillis();

  abstract String tableName();

  abstract ImmutableList<PartitionColumn> partitionColumns();

  abstract Long approxTotalRowCount();

  abstract Long maxPartitionsHint();

  abstract Boolean autoAdjustMaxPartitions();

  abstract Long initialSplitHint();

  abstract Long splitStageCountHint();

  abstract JdbcIO.RowMapper<T> rowMapper();

  @Nullable
  abstract PTransform<PCollection<ImmutableList<Range>>, ?> rangesPeek();

  @Nullable
  abstract List<PCollection<?>> waitOnSignals();

  @Nullable
  abstract Range initialRange();

  @Override
  public PCollection<T> expand(PBegin input) {
    // TODO(vardhanvthigle): Implement mapper for strings of various collations as side input.
    BoundaryTypeMapper typeMapper = null;
    // Generate Initial Ranges with liner splits (No need to do execute count queries the DB here)
    PCollection<ImmutableList<Range>> ranges = initialSplit(input, typeMapper);
    // Classify Ranges to count or get new columns and perform the operations.
    for (long i = 0; i < splitStageCountHint(); i++) {
      ranges = addStage(ranges, typeMapper, i);
    }

    ImmutableList<String> colNames =
        partitionColumns().stream()
            .map(PartitionColumn::columnName)
            .collect(ImmutableList.toImmutableList());
    PreparedStatementSetter<Range> rangePrepareator =
        new RangePreparedStatementSetter(colNames.size());

    // Merge the Ranges towards the mean.
    PCollection<ImmutableList<Range>> mergedRanges =
        ranges.apply(
            getTransformName("RangeMerge", null),
            ParDo.of(
                MergeRangesDoFn.builder()
                    .setApproxTotalRowCount(approxTotalRowCount())
                    .setMaxPartitionHint(maxPartitionsHint())
                    .setAutoAdjustMaxPartitions(autoAdjustMaxPartitions())
                    .setTableName(tableName())
                    .build()));
    PCollection<Range> rangesToRead =
        peekRanges(mergedRanges).apply(ParDo.of(new UnflattenRangesDoFn()));
    return rangesToRead.apply(
        getTransformName("RangeRead", null),
        JdbcIO.<Range, T>readAll()
            .withOutputParallelization(false)
            .withQuery(dbAdapter().getReadQuery(tableName(), colNames))
            .withParameterSetter(rangePrepareator)
            .withDataSourceProviderFn(dataSourceProviderFn())
            .withRowMapper(rowMapper()));
  }

  public static <T> Builder<T> builder() {
    return new AutoValue_ReadWithUniformPartitions.Builder<T>()
        .setCountQueryTimeoutMillis(SPLITTER_DEFAULT_COUNT_QUERY_TIMEOUT_MILLIS)
        .setAutoAdjustMaxPartitions(true);
  }

  /** Gives log to the base 2 of a given value rounded up. */
  @VisibleForTesting
  protected static long logToBaseTwo(long value) {
    return value == 0L ? 0L : 64L - Long.numberOfLeadingZeros(value - 1);
  }

  private PCollection<ImmutableList<Range>> initialSplit(
      PBegin input, @Nullable BoundaryTypeMapper typeMapper) {

    RangeBoundaryTransform rangeBoundaryTransform =
        RangeBoundaryTransform.builder()
            .setBoundaryTypeMapper(typeMapper)
            .setDataSourceProviderFn(dataSourceProviderFn())
            .setDbAdapter(dbAdapter())
            .setPartitionColumns(
                partitionColumns().stream()
                    .map(c -> c.columnName())
                    .collect(ImmutableList.toImmutableList()))
            .setTableName(tableName())
            .build();

    long splitHeight = logToBaseTwo(initialSplitHint());

    PCollection<Range> initialRange;
    if (initialRange() == null) {
      ColumnForBoundaryQuery initialColumn =
          ColumnForBoundaryQuery.builder()
              .setPartitionColumn(partitionColumns().get(0))
              .setParentRange(null)
              .build();
      initialRange =
          wait(input.apply(Create.of(ImmutableList.of(initialColumn))))
              .apply(rangeBoundaryTransform);
    } else {
      initialRange = wait(input.apply(Create.of(ImmutableList.of(initialRange()))));
    }

    return initialRange.apply(
        getTransformName("InitialRangeSplit", null),
        ParDo.of(
            InitialSplitRangeDoFn.builder()
                .setSplitHeight(splitHeight)
                .setTableName(tableName())
                .build()));
  }

  private PCollection<ImmutableList<Range>> addStage(
      PCollection<ImmutableList<Range>> input, BoundaryTypeMapper typeMapper, long stageIdx) {

    RangeClassifierDoFn classifierFn =
        RangeClassifierDoFn.builder()
            .setApproxTotalRowCount(approxTotalRowCount())
            .setMaxPartitionHint(maxPartitionsHint())
            .setAutoAdjustMaxPartitions(autoAdjustMaxPartitions())
            .setPartitionColumns(partitionColumns())
            .setStageIdx(stageIdx)
            .build();

    RangeCountTransform rangeCountTransform =
        RangeCountTransform.builder()
            .setDataSourceProviderFn(dataSourceProviderFn())
            .setDbAdapter(dbAdapter())
            .setPartitionColumns(
                partitionColumns().stream()
                    .map(c -> c.columnName())
                    .collect(ImmutableList.toImmutableList()))
            .setTableName(tableName())
            .setTimeoutMillis(countQueryTimeoutMillis())
            .build();

    RangeBoundaryTransform rangeBoundaryTransform =
        RangeBoundaryTransform.builder()
            .setDataSourceProviderFn(dataSourceProviderFn())
            .setBoundaryTypeMapper(typeMapper)
            .setDbAdapter(dbAdapter())
            .setPartitionColumns(
                partitionColumns().stream()
                    .map(c -> c.columnName())
                    .collect(ImmutableList.toImmutableList()))
            .setTableName(tableName())
            .build();
    PCollectionTuple classifiedRanges =
        input.apply(
            getTransformName("RangeClassifier", stageIdx),
            ParDo.of(classifierFn)
                .withOutputTags(
                    RangeClassifierDoFn.TO_COUNT_TAG,
                    TupleTagList.of(RangeClassifierDoFn.TO_ADD_COLUMN_TAG)
                        .and(RangeClassifierDoFn.TO_RETAIN_TAG)));
    PCollection<Range> countedRanges =
        classifiedRanges
            .get(RangeClassifierDoFn.TO_COUNT_TAG)
            .apply(getTransformName("RangeCounter", stageIdx), rangeCountTransform);
    PCollection<ColumnForBoundaryQuery> rangesToAddColumn =
        classifiedRanges.get(RangeClassifierDoFn.TO_ADD_COLUMN_TAG);
    PCollection<Range> rangesWithNewColumns =
        rangesToAddColumn
            .apply(getTransformName("RangeBoundary", stageIdx), rangeBoundaryTransform)
            .apply(
                getTransformName("RangeSplitterAfterColumnAddition", stageIdx),
                ParDo.of(new SplitRangeDoFn()))
            .apply(getTransformName("CountAfterColumnAddition", stageIdx), rangeCountTransform);
    PCollection<Range> retainedRanges = classifiedRanges.get(RangeClassifierDoFn.TO_RETAIN_TAG);

    return PCollectionList.of(retainedRanges)
        .and(countedRanges)
        .and(rangesWithNewColumns)
        .apply(getTransformName("FlattenProcessedRanges", stageIdx), Flatten.pCollections())
        .apply(getTransformName("RangeCombiner", stageIdx), RangeCombiner.globally());
  }

  private String getTransformName(String transformType, @Nullable Long stageIdx) {
    String name = "Table." + tableName() + "." + transformType;
    if (stageIdx != null) {
      name = name + "." + stageIdx;
    }
    return name;
  }

  private <V extends PCollection> V wait(V input) {
    if (waitOnSignals() == null) {
      return input;
    } else {
      return (V) input.apply(Wait.on(waitOnSignals()));
    }
  }

  private PCollection<ImmutableList<Range>> peekRanges(
      PCollection<ImmutableList<Range>> mergedRanges) {
    if (rangesPeek() == null) {
      return mergedRanges;
    } else {
      return mergedRanges.apply(Wait.on((PCollection<?>) mergedRanges.apply(rangesPeek())));
    }
  }

  public static long inferMaxPartitions(long count) {
    return Math.max(1, Math.round(Math.floor(Math.sqrt(count) / 10)));
  }

  @AutoValue.Builder
  public abstract static class Builder<T> {

    public abstract Builder<T> setDataSourceProviderFn(
        SerializableFunction<Void, DataSource> value);

    public abstract Builder<T> setDbAdapter(UniformSplitterDBAdapter value);

    public abstract Builder<T> setCountQueryTimeoutMillis(long value);

    public abstract Builder<T> setTableName(String value);

    public abstract Builder<T> setPartitionColumns(ImmutableList<PartitionColumn> value);

    abstract ImmutableList<PartitionColumn> partitionColumns();

    public abstract Builder<T> setApproxTotalRowCount(Long value);

    abstract Long approxTotalRowCount();

    public abstract Builder<T> setMaxPartitionsHint(Long value);

    public abstract Builder<T> setAutoAdjustMaxPartitions(Boolean value);

    abstract Optional<Long> maxPartitionsHint();

    public abstract Builder<T> setInitialSplitHint(Long value);

    abstract Optional<Long> initialSplitHint();

    public abstract Builder<T> setSplitStageCountHint(Long value);

    public abstract Builder<T> setRowMapper(JdbcIO.RowMapper<T> value);

    public abstract Builder<T> setRangesPeek(
        @Nullable PTransform<PCollection<ImmutableList<Range>>, ?> value);

    public abstract Builder<T> setWaitOnSignals(@Nullable List<PCollection<?>> value);

    public abstract Builder<T> setInitialRange(Range value);

    @Nullable
    abstract Range initialRange();

    abstract Optional<Long> splitStageCountHint();

    abstract ReadWithUniformPartitions<T> autoBuild();

    public ReadWithUniformPartitions<T> build() {
      /* TODO(vardhanvthigle): USE `Explain Select *` to auto infer approxCount within the uniform partition splitter itself.
         Since that will be executed in construction path, it needs to be retried with backoff.
         For the context of the larger reader, the index cardinality gives a pretty good idea of the approxCount of the table too.
      */
      long maxPartitionsHint;
      if (maxPartitionsHint().isPresent()) {
        maxPartitionsHint = maxPartitionsHint().get();
      } else {
        maxPartitionsHint = inferMaxPartitions(approxTotalRowCount());
        setMaxPartitionsHint(maxPartitionsHint);
      }
      if (!initialSplitHint().isPresent()) {
        setInitialSplitHint(SPLITTER_MAX_RELATIVE_DEVIATION * maxPartitionsHint);
      }
      if (!splitStageCountHint().isPresent()) {
        Long splitHeightHint =
            logToBaseTwo(maxPartitionsHint)
                + partitionColumns().size()
                + 1 /* For initial counts */;
        setSplitStageCountHint(splitHeightHint);
      }

      if (initialRange() != null) {
        Range curRange = initialRange();
        for (PartitionColumn col : partitionColumns()) {
          Preconditions.checkState(curRange.colName().equals(col.columnName()));
          if (curRange.hasChildRange()) {
            curRange = curRange.childRange();
          } else {
            break;
          }
        }
      }
      ReadWithUniformPartitions readWithUniformPartitions = autoBuild();
      logger.info("Initialized ReadWithUniformPartitions {}", readWithUniformPartitions);
      return readWithUniformPartitions;
    }
  }
}
