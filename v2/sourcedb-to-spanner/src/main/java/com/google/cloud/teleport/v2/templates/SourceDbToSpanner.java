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
package com.google.cloud.teleport.v2.templates;


import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.coders.JsonNodeCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.ResultSetToJsonNode;
import com.google.cloud.teleport.v2.source.DataSourceProvider;
import com.google.cloud.teleport.v2.spanner.ProcessInformationSchema;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.transformer.JsonNodeToMutationDoFn;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.HashMap;
import java.util.Map;

/**
 * A template that copies data from a relational database using JDBC to an existing Spanner
 * database.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/README_Sourcedb_to_Spanner_Flex.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
        name = "Sourcedb_to_Spanner_Flex",
        category = TemplateCategory.BATCH,
        displayName = "Sourcedb to Spanner",
        description = {
                "The SourceDB to Spanner template is a batch pipeline that copies data from a relational"
                        + " database into an existing Spanner database. This pipeline uses JDBC to connect to"
                        + " the relational database. You can use this template to copy data from any relational"
                        + " database with available JDBC drivers into Spanner. This currently only supports a limited set of types of MySQL",
                "For an extra layer of protection, you can also pass in a Cloud KMS key along with a"
                        + " Base64-encoded username, password, and connection string parameters encrypted with"
                        + " the Cloud KMS key. See the <a"
                        + " href=\"https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt\">Cloud"
                        + " KMS API encryption endpoint</a> for additional details on encrypting your username,"
                        + " password, and connection string parameters."
        },
        optionsClass = SourceDbToSpannerOptions.class,
        flexContainerName = "source-db-to-spanner",
        documentation =
                "https://cloud.google.com/dataflow/docs/guides/templates/provided/sourcedb-to-spanner",
        contactInformation = "https://cloud.google.com/support",
        preview = true,
        requirements = {
                "The JDBC drivers for the relational database must be available.",
                "The Spanner tables must exist before pipeline execution.",
                "The Spanner tables must have a compatible schema.",
                "The relational database must be accessible from the subnet where Dataflow runs."
        })
public class SourceDbToSpanner {

    /**
     * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
     * blocking execution is required, use the {@link SourceDbToSpanner#run} method to start the
     * pipeline and invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line arguments to the pipeline.
     */
    public static void main(String[] args) {
        UncaughtExceptionLogger.register();

        // Parse the user options passed from the command-line
        SourceDbToSpannerOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(SourceDbToSpannerOptions.class);

        run(options);
    }

    /**
     * Create the pipeline with the supplied options.
     *
     * @param options The execution parameters to the pipeline.
     * @return The result of the pipeline execution.
     */
    @VisibleForTesting
    static PipelineResult run(SourceDbToSpannerOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        Map<String, String> tableVsPartitionMap = getTablesVsPartitionColumn(options);
        // Ingest session file into schema object.
        Schema schema = SessionFileReader.read(options.getSessionFilePath());

        SpannerConfig spannerConfig =
                SpannerConfig.create()
                        .withProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()))
                        .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
                        .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
                        .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()));

        Ddl ddl = ProcessInformationSchema.getInformationSchemaAsDdl(spannerConfig);
        for (String table : getTablesVsPartitionColumn(options).keySet()) {
            PCollection<Mutation> rows =
                    pipeline
                            .apply(
                                    "ReadPartitions_" + table,
                                    getJdbcReader(table, tableVsPartitionMap.get(table), options))
                            .setCoder(JsonNodeCoder.of())
                            .apply(
                                    "Map JsonNode to Mutations",
                                    ParDo.of(
                                            new JsonNodeToMutationDoFn(
                                                    schema, table, ddl)));
            rows.apply("Write_" + table, getSpannerWrite(options));
        }
        return pipeline.run();
    }

    private static Map<String, String> getTablesVsPartitionColumn(SourceDbToSpannerOptions options) {
        String[] tables = options.getTables().split(",");
        String[] partitionColumns = options.getPartitionColumns().split(",");
        if (tables.length != partitionColumns.length) {
            throw new RuntimeException(
                    "invalid configuration. Partition column count does not match " + "tables count.");
        }
        Map<String, String> tableVsPartitionColumn = new HashMap();
        for (int i = 0; i < tables.length; i++) {
            tableVsPartitionColumn.put(tables[i], partitionColumns[i]);
        }
        return tableVsPartitionColumn;
    }

    private static JdbcIO.ReadWithPartitions<JsonNode, Long> getJdbcReader(
            String table, String partitionColumn, SourceDbToSpannerOptions options) {
        return JdbcIO.<JsonNode>readWithPartitions()
                .withDataSourceProviderFn(new DataSourceProvider(options))
                .withTable(table)
                .withPartitionColumn(partitionColumn)
                .withRowMapper(ResultSetToJsonNode.create(table))
                .withCoder(JsonNodeCoder.of())
                .withNumPartitions(options.getNumPartitions());
    }

    private static Write getSpannerWrite(SourceDbToSpannerOptions options) {
        return SpannerIO.write()
                .withProjectId(options.getProjectId())
                .withInstanceId(options.getInstanceId())
                .withDatabaseId(options.getDatabaseId());
    }
}
