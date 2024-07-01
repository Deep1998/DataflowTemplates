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
package com.google.cloud.teleport.v2.source.reader.io.jdbc;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.jdbc.JdbcIO.ReadWithPartitions;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LocalJdbcUtil {
  /**
   * A helper for {@link ReadWithPartitions} that handles range calculations.
   *
   * @param <PartitionT>
   */
  interface JdbcReadWithPartitionsHelper<PartitionT>
      extends PreparedStatementSetter<KV<PartitionT, PartitionT>>,
          RowMapper<KV<Long, KV<PartitionT, PartitionT>>> {
    static <T> @Nullable JdbcReadWithPartitionsHelper<T> getPartitionsHelper(
        TypeDescriptor<T> type) {
      // This cast is unchecked, thus this is a small type-checking risk. We just need
      // to make sure that all preset helpers in `JdbcUtil.PRESET_HELPERS` are matched
      // in type from their Key and their Value.
      return (JdbcReadWithPartitionsHelper<T>) PRESET_HELPERS.get(type.getRawType());
    }

    Iterable<KV<PartitionT, PartitionT>> calculateRanges(
        PartitionT lowerBound, PartitionT upperBound, Long partitions);

    @Override
    void setParameters(KV<PartitionT, PartitionT> element, PreparedStatement preparedStatement);

    @Override
    KV<Long, KV<PartitionT, PartitionT>> mapRow(ResultSet resultSet) throws Exception;
  }

  /** Create partitions on a table. */
  static class PartitioningFn<T> extends DoFn<KV<Long, KV<T, T>>, KV<T, T>> {
    private static final Logger LOG = LoggerFactory.getLogger(PartitioningFn.class);
    final TypeDescriptor<T> partitioningColumnType;

    PartitioningFn(TypeDescriptor<T> partitioningColumnType) {
      this.partitioningColumnType = partitioningColumnType;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      T lowerBound = c.element().getValue().getKey();
      T upperBound = c.element().getValue().getValue();
      JdbcReadWithPartitionsHelper<T> helper =
          checkStateNotNull(
              JdbcReadWithPartitionsHelper.getPartitionsHelper(partitioningColumnType));
      List<KV<T, T>> ranges =
          Lists.newArrayList(helper.calculateRanges(lowerBound, upperBound, c.element().getKey()));
      LOG.warn("Total of {} ranges: {}", ranges.size(), ranges);
      for (KV<T, T> e : ranges) {
        c.output(e);
      }
    }
  }

  public static final Map<Class<?>, JdbcReadWithPartitionsHelper<?>> PRESET_HELPERS =
      ImmutableMap.of(
          Long.class,
          new JdbcReadWithPartitionsHelper<Long>() {
            @Override
            public Iterable<KV<Long, Long>> calculateRanges(
                Long lowerBound, Long upperBound, Long partitions) {
              List<KV<Long, Long>> ranges = new ArrayList<>();
              // We divide by partitions FIRST to make sure that we can cover the whole LONG range.
              // If we substract first, then we may end up with Long.MAX - Long.MIN, which is 2*MAX,
              // and we'd have trouble with the pipeline.
              long stride = (upperBound / partitions - lowerBound / partitions) + 1;
              long highest = lowerBound;
              for (long i = lowerBound; i < upperBound - stride; i += stride) {
                ranges.add(KV.of(i, i + stride));
                highest = i + stride;
              }
              if (highest < upperBound + 1) {
                ranges.add(KV.of(highest, upperBound + 1));
              }
              return ranges;
            }

            @Override
            public void setParameters(KV<Long, Long> element, PreparedStatement preparedStatement) {
              try {
                preparedStatement.setLong(1, element.getKey());
                preparedStatement.setLong(2, element.getValue());
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public KV<Long, KV<Long, Long>> mapRow(ResultSet resultSet) throws Exception {
              if (resultSet.getMetaData().getColumnCount() == 3) {
                return KV.of(
                    resultSet.getLong(3), KV.of(resultSet.getLong(1), resultSet.getLong(2)));
              } else {
                return KV.of(0L, KV.of(resultSet.getLong(1), resultSet.getLong(2)));
              }
            }
          },
          DateTime.class,
          new JdbcReadWithPartitionsHelper<DateTime>() {
            @Override
            public Iterable<KV<DateTime, DateTime>> calculateRanges(
                DateTime lowerBound, DateTime upperBound, Long partitions) {
              final List<KV<DateTime, DateTime>> result = new ArrayList<>();

              final long intervalMillis = upperBound.getMillis() - lowerBound.getMillis();
              final Duration stride = Duration.millis(Math.max(1, intervalMillis / partitions));
              // Add the first advancement
              DateTime currentLowerBound = lowerBound;
              // Zero output in a comparison means that elements are equal
              while (currentLowerBound.compareTo(upperBound) <= 0) {
                DateTime currentUpper = currentLowerBound.plus(stride);
                if (currentUpper.compareTo(upperBound) >= 0) {
                  // If we hit the upper bound directly, then we want to be just-above it, so that
                  // it will be captured by the less-than query.
                  currentUpper = upperBound.plusMillis(1);
                  result.add(KV.of(currentLowerBound, currentUpper));
                  return result;
                }
                result.add(KV.of(currentLowerBound, currentUpper));
                currentLowerBound = currentLowerBound.plus(stride);
              }
              return result;
            }

            @Override
            public void setParameters(
                KV<DateTime, DateTime> element, PreparedStatement preparedStatement) {
              try {
                preparedStatement.setTimestamp(1, new Timestamp(element.getKey().getMillis()));
                preparedStatement.setTimestamp(2, new Timestamp(element.getValue().getMillis()));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public KV<Long, KV<DateTime, DateTime>> mapRow(ResultSet resultSet) throws Exception {
              if (resultSet.getMetaData().getColumnCount() == 3) {
                return KV.of(
                    resultSet.getLong(3),
                    KV.of(
                        new DateTime(checkArgumentNotNull(resultSet.getTimestamp(1))),
                        new DateTime(checkArgumentNotNull(resultSet.getTimestamp(2)))));
              } else {
                return KV.of(
                    0L,
                    KV.of(
                        new DateTime(checkArgumentNotNull(resultSet.getTimestamp(1))),
                        new DateTime(checkArgumentNotNull(resultSet.getTimestamp(2)))));
              }
            }
          });
}
