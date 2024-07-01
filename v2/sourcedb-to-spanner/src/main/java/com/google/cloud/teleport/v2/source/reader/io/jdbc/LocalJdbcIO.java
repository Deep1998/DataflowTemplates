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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.SchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalJdbcIO {
  private static final Logger LOG = LoggerFactory.getLogger(LocalJdbcIO.class);

  private static final int DEFAULT_FETCH_SIZE = 50_000;
  // Default value used for partitioning a table
  private static final int DEFAULT_NUM_PARTITIONS = 200;

  public static <T, PartitionColumnT>
      LocalReadWithPartitions<T, PartitionColumnT> localReadWithPartitions(
          TypeDescriptor<PartitionColumnT> partitioningColumnType) {
    return new AutoValue_LocalJdbcIO_LocalReadWithPartitions.Builder<T, PartitionColumnT>()
        .setPartitionColumnType(partitioningColumnType)
        .setNumPartitions(DEFAULT_NUM_PARTITIONS)
        .setFetchSize(DEFAULT_FETCH_SIZE)
        .setUseBeamSchema(false)
        .build();
  }

  public static <T> LocalReadWithPartitions<T, Long> localReadWithPartitions() {
    return LocalJdbcIO.<T, Long>localReadWithPartitions(TypeDescriptors.longs());
  }

  @AutoValue
  public abstract static class LocalReadWithPartitions<T, PartitionColumnT>
      extends PTransform<PBegin, PCollection<T>> {

    @Pure
    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Pure
    abstract @Nullable LocalJdbcRowMapper<T> getRowMapper();

    @Pure
    abstract @Nullable Coder<T> getCoder();

    @Pure
    abstract @Nullable Integer getNumPartitions();

    @Pure
    abstract @Nullable String getPartitionColumn();

    @Pure
    abstract int getFetchSize();

    @Pure
    abstract boolean getUseBeamSchema();

    @Pure
    abstract @Nullable PartitionColumnT getLowerBound();

    @Pure
    abstract @Nullable PartitionColumnT getUpperBound();

    @Pure
    abstract @Nullable String getTable();

    @Pure
    abstract TypeDescriptor<PartitionColumnT> getPartitionColumnType();

    @Pure
    abstract @Nullable PCollection<?> getParentPCollection();

    @Pure
    abstract Builder<T, PartitionColumnT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T, PartitionColumnT> {

      abstract Builder<T, PartitionColumnT> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T, PartitionColumnT> setRowMapper(LocalJdbcRowMapper<T> rowMapper);

      abstract Builder<T, PartitionColumnT> setCoder(Coder<T> coder);

      abstract Builder<T, PartitionColumnT> setNumPartitions(int numPartitions);

      abstract Builder<T, PartitionColumnT> setPartitionColumn(String partitionColumn);

      abstract Builder<T, PartitionColumnT> setLowerBound(PartitionColumnT lowerBound);

      abstract Builder<T, PartitionColumnT> setUpperBound(PartitionColumnT upperBound);

      abstract Builder<T, PartitionColumnT> setUseBeamSchema(boolean useBeamSchema);

      abstract Builder<T, PartitionColumnT> setFetchSize(int fetchSize);

      abstract Builder<T, PartitionColumnT> setTable(String tableName);

      abstract Builder<T, PartitionColumnT> setParentPCollection(PCollection<?> parent);

      abstract Builder<T, PartitionColumnT> setPartitionColumnType(
          TypeDescriptor<PartitionColumnT> partitionColumnType);

      abstract LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> build();
    }

    public LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> withRowMapper(
        LocalJdbcRowMapper<T> rowMapper) {
      checkNotNull(rowMapper, "rowMapper can not be null");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    public LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> withParentPCollection(
        PCollection<?> parent) {
      return toBuilder().setParentPCollection(parent).build();
    }

    /**
     * @deprecated
     *     <p>{@link JdbcIO} is able to infer appropriate coders from other parameters.
     */
    @Deprecated
    public LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> withCoder(Coder<T> coder) {
      checkNotNull(coder, "coder can not be null");
      return toBuilder().setCoder(coder).build();
    }

    /**
     * The number of partitions. This, along with withLowerBound and withUpperBound, form partitions
     * strides for generated WHERE clause expressions used to split the column withPartitionColumn
     * evenly. When the input is less than 1, the number is set to 1.
     */
    public LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> withNumPartitions(
        int numPartitions) {
      checkArgument(numPartitions > 0, "numPartitions can not be less than 1");
      return toBuilder().setNumPartitions(numPartitions).build();
    }

    /** The name of a column of numeric type that will be used for partitioning. */
    public LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> withPartitionColumn(
        String partitionColumn) {
      checkNotNull(partitionColumn, "partitionColumn can not be null");
      return toBuilder().setPartitionColumn(partitionColumn).build();
    }

    /** The number of rows to fetch from the database in the same {@link ResultSet} round-trip. */
    public LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> withFetchSize(int fetchSize) {
      checkArgument(fetchSize > 0, "fetchSize can not be less than 1");
      return toBuilder().setFetchSize(fetchSize).build();
    }

    /** Data output type is {@link Row}, and schema is auto-inferred from the database. */
    public LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> withRowOutput() {
      return toBuilder().setUseBeamSchema(true).build();
    }

    public LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> withLowerBound(
        PartitionColumnT lowerBound) {
      return toBuilder().setLowerBound(lowerBound).build();
    }

    public LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> withUpperBound(
        PartitionColumnT upperBound) {
      return toBuilder().setUpperBound(upperBound).build();
    }

    /** Name of the table in the external database. Can be used to pass a user-defined subqery. */
    public LocalJdbcIO.LocalReadWithPartitions<T, PartitionColumnT> withTable(String tableName) {
      checkNotNull(tableName, "table can not be null");
      return toBuilder().setTable(tableName).build();
    }

    private static final int EQUAL = 0;

    @Override
    public PCollection<T> expand(PBegin input) {
      SerializableFunction<Void, DataSource> dataSourceProviderFn =
          checkStateNotNull(
              getDataSourceProviderFn(),
              "withDataSourceConfiguration() or withDataSourceProviderFn() is required");
      String partitionColumn =
          checkStateNotNull(getPartitionColumn(), "withPartitionColumn() is required");
      String table = checkStateNotNull(getTable(), "withTable() is required");
      checkArgument(
          // We XOR so that only one of these is true / provided. (^ is an xor operator : ))
          getUseBeamSchema() ^ getRowMapper() != null,
          "Provide only withRowOutput() or withRowMapper() arguments for "
              + "LocalJdbcIO.LocalReadWithPartitions). These are mutually exclusive.");
      checkArgument(
          (getUpperBound() != null) == (getLowerBound() != null),
          "When providing either lower or upper bound, both "
              + "parameters are mandatory for LocalJdbcIO.LocalReadWithPartitions");
      if (getLowerBound() != null
          && getUpperBound() != null
          && getLowerBound() instanceof Comparable<?>) {
        // Not all partition types are comparable. For example, LocalDateTime, which is a valid
        // partitioning type, is not Comparable, so we can't enforce this for all sorts of
        // partitioning.
        checkArgument(
            ((Comparable<PartitionColumnT>) getLowerBound()).compareTo(getUpperBound()) < EQUAL,
            "The lower bound of partitioning column is larger or equal than the upper bound");
      }
      checkNotNull(
          LocalJdbcUtil.JdbcReadWithPartitionsHelper.getPartitionsHelper(getPartitionColumnType()),
          "localReadWithPartitions only supports the following types: %s",
          LocalJdbcUtil.PRESET_HELPERS.keySet());

      PCollection<KV<Long, KV<PartitionColumnT, PartitionColumnT>>> params;

      if (getLowerBound() == null && getUpperBound() == null) {
        String query =
            String.format(
                "SELECT min(%s), max(%s) FROM %s", partitionColumn, partitionColumn, table);
        if (getNumPartitions() == null) {
          query =
              String.format(
                  "SELECT min(%s), max(%s), count(*) FROM %s",
                  partitionColumn, partitionColumn, table);
        }
        params =
            input
                .apply(
                    JdbcIO.<KV<Long, KV<PartitionColumnT, PartitionColumnT>>>read()
                        .withQuery(query)
                        .withDataSourceProviderFn(dataSourceProviderFn)
                        .withRowMapper(
                            checkStateNotNull(
                                LocalJdbcUtil.JdbcReadWithPartitionsHelper.getPartitionsHelper(
                                    getPartitionColumnType())))
                        .withFetchSize(getFetchSize()))
                .apply(
                    MapElements.via(
                        new SimpleFunction<
                            KV<Long, KV<PartitionColumnT, PartitionColumnT>>,
                            KV<Long, KV<PartitionColumnT, PartitionColumnT>>>() {
                          @Override
                          public KV<Long, KV<PartitionColumnT, PartitionColumnT>> apply(
                              KV<Long, KV<PartitionColumnT, PartitionColumnT>> input) {
                            KV<Long, KV<PartitionColumnT, PartitionColumnT>> result;
                            if (getNumPartitions() == null) {
                              // In this case, we use the table row count to infer a number of
                              // partitions.
                              // We take the square root of the number of rows, and divide it by 10
                              // to keep a relatively low number of partitions, given that an RDBMS
                              // cannot usually accept a very large number of connections.
                              long numPartitions =
                                  Math.max(
                                      1, Math.round(Math.floor(Math.sqrt(input.getKey()) / 10)));
                              result = KV.of(numPartitions, input.getValue());
                            } else {
                              result = KV.of(getNumPartitions().longValue(), input.getValue());
                            }
                            LOG.info(
                                "Inferred min: {} - max: {} - numPartitions: {}",
                                result.getValue().getKey(),
                                result.getValue().getValue(),
                                result.getKey());
                            return result;
                          }
                        }));
      } else {
        params =
            input.apply(
                Create.of(
                    KV.of(
                        checkStateNotNull(getNumPartitions()).longValue(),
                        KV.of(getLowerBound(), getUpperBound()))));
      }

      JdbcIO.RowMapper<T> rowMapper = null;
      Schema schema = null;
      if (getUseBeamSchema()) {
        schema =
            JdbcIO.ReadRows.inferBeamSchema(
                dataSourceProviderFn.apply(null), String.format("SELECT * FROM %s", getTable()));
        rowMapper = (JdbcIO.RowMapper<T>) SchemaUtil.BeamRowMapper.of(schema);
      } else {
        rowMapper = getRowMapper();
      }
      checkStateNotNull(rowMapper);

      PCollection<KV<PartitionColumnT, PartitionColumnT>> ranges =
          params
              .apply(
                  "Partitioning",
                  ParDo.of(new LocalJdbcUtil.PartitioningFn<>(getPartitionColumnType())))
              .apply("Reshuffle partitions", Reshuffle.viaRandomKey());

      JdbcIO.ReadAll<KV<PartitionColumnT, PartitionColumnT>, T> readAll =
          JdbcIO.<KV<PartitionColumnT, PartitionColumnT>, T>readAll()
              .withDataSourceProviderFn(dataSourceProviderFn)
              .withQuery(
                  String.format(
                      "select * from %1$s where %2$s >= ? and %2$s < ?", table, partitionColumn))
              .withRowMapper(rowMapper)
              .withFetchSize(getFetchSize())
              .withParameterSetter(
                  checkStateNotNull(
                          LocalJdbcUtil.JdbcReadWithPartitionsHelper.getPartitionsHelper(
                              getPartitionColumnType()))
                      ::setParameters)
              .withOutputParallelization(false);

      if (getUseBeamSchema()) {
        checkStateNotNull(schema);
        readAll = readAll.withCoder((Coder<T>) RowCoder.of(schema));
      } else if (getCoder() != null) {
        readAll = readAll.withCoder(getCoder());
      }

      if (getParentPCollection() != null) {
        ranges = ranges.apply(Wait.on(getParentPCollection()));
      }
      return ranges.apply("Read ranges", readAll);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(
          DisplayData.item(
              "rowMapper",
              getRowMapper() == null
                  ? "auto-infer"
                  : getRowMapper().getClass().getCanonicalName()));
      if (getCoder() != null) {
        builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      }
      builder.add(DisplayData.item("partitionColumn", getPartitionColumn()));
      builder.add(DisplayData.item("table", getTable()));
      builder.add(
          DisplayData.item(
              "numPartitions",
              getNumPartitions() == null ? "auto-infer" : getNumPartitions().toString()));
      builder.add(
          DisplayData.item(
              "lowerBound", getLowerBound() == null ? "auto-infer" : getLowerBound().toString()));
      builder.add(
          DisplayData.item(
              "upperBound", getUpperBound() == null ? "auto-infer" : getUpperBound().toString()));
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }
}
