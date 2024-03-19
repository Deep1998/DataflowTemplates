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
package com.google.cloud.teleport.v2.spanner.migrations.convertors;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.spanner.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.EVENT_METADATA_KEY_PREFIX;
import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.EVENT_SCHEMA_KEY;
import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.EVENT_TABLE_NAME_KEY;
import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.EVENT_UUID_KEY;
import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;

/**
 * Utility class with methods which converts change events based on {@link com.google.cloud.teleport.v2.spanner.migrations.schema}.
 */
public class ChangeEventSessionConvertor {
    // The mapping information read from the session file generated by HarbourBridge.
    private final Schema schema;

    /* The context used to populate transformation information */
    private TransformationContext transformationContext = null;

    // The source database type.
    private String sourceType = null;

    // If set to true, round decimals inside jsons.
    private Boolean roundJsonDecimals = false;

    // If set to true, generate a UUID instead of using the one inside change event. Valid for synthetic PK use cases.
    private Boolean generateUUID = false;

    public ChangeEventSessionConvertor(Schema schema, TransformationContext transformationContext, String sourceType, boolean roundJsonDecimals) {
        this.schema = schema;
        this.transformationContext = transformationContext;
        this.sourceType = sourceType;
        this.roundJsonDecimals = roundJsonDecimals;
    }

    public ChangeEventSessionConvertor(Schema schema) {
        this.schema = schema;
    }

    public void shouldGenerateUUID(boolean generateUUID) {
        this.generateUUID = generateUUID;
    }

    /**
     * This function modifies the change event using transformations based on the session file (stored
     * in the Schema object). This includes column/table name changes and adding of synthetic Primary
     * Keys.
     */
    public JsonNode transformChangeEventViaSessionFile(JsonNode changeEvent) {
        String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
        String tableId = schema.getSrcToID().get(tableName).getName();

        // Convert table and column names in change event.
        changeEvent = convertTableAndColumnNames(changeEvent, tableName);

        // Add synthetic PK to change event.
        changeEvent = addSyntheticPKs(changeEvent, tableId);

        // Remove columns present in change event that were dropped in Spanner.
        changeEvent = removeDroppedColumns(changeEvent, tableId);

        if (transformationContext != null) {
            // Add shard id to change event.
            changeEvent = populateShardId(changeEvent, tableId);
        }

        return changeEvent;
    }

    JsonNode populateShardId(JsonNode changeEvent, String tableId) {
        if (!MYSQL_SOURCE_TYPE.equals(this.sourceType)
                || transformationContext.getSchemaToShardId() == null
                || transformationContext.getSchemaToShardId().isEmpty()) {
            return changeEvent; // Nothing to do
        }

        SpannerTable table = schema.getSpSchema().get(tableId);
        String shardIdColumn = table.getShardIdColumn();
        if (shardIdColumn == null) {
            return changeEvent;
        }
        SpannerColumnDefinition shardIdColDef = table.getColDefs().get(table.getShardIdColumn());
        if (shardIdColDef == null) {
            return changeEvent;
        }
        Map<String, String> schemaToShardId = transformationContext.getSchemaToShardId();
        String schemaName = changeEvent.get(EVENT_SCHEMA_KEY).asText();
        String shardId = schemaToShardId.get(schemaName);
        ((ObjectNode) changeEvent).put(shardIdColDef.getName(), shardId);
        return changeEvent;
    }

    JsonNode convertTableAndColumnNames(JsonNode changeEvent, String tableName) {
        NameAndCols nameAndCols = schema.getToSpanner().get(tableName);
        String spTableName = nameAndCols.getName();
        Map<String, String> cols = nameAndCols.getCols();

        // Convert the table name to corresponding Spanner table name.
        ((ObjectNode) changeEvent).put(EVENT_TABLE_NAME_KEY, spTableName);
        // Convert the column names to corresponding Spanner column names.
        for (Map.Entry<String, String> col : cols.entrySet()) {
            String srcCol = col.getKey(), spCol = col.getValue();
            if (!srcCol.equals(spCol)) {
                ((ObjectNode) changeEvent).set(spCol, changeEvent.get(srcCol));
                ((ObjectNode) changeEvent).remove(srcCol);
            }
        }
        return changeEvent;
    }

    JsonNode addSyntheticPKs(JsonNode changeEvent, String tableId) {
        Map<String, SpannerColumnDefinition> spCols = schema.getSpSchema().get(tableId).getColDefs();
        Map<String, SyntheticPKey> synthPks = schema.getSyntheticPks();
        if (synthPks.containsKey(tableId)) {
            String colID = synthPks.get(tableId).getColId();
            if (!spCols.containsKey(colID)) {
                throw new IllegalArgumentException(
                        "Missing entry for "
                                + colID
                                + " in colDefs for tableId: "
                                + tableId
                                + ", provide a valid session file.");
            }
            String uuid = "";
            if (generateUUID) {
                uuid = UUID.randomUUID().toString();
            } else {
                uuid = changeEvent.get(EVENT_UUID_KEY).asText();
            }
            ((ObjectNode) changeEvent)
                    .put(spCols.get(colID).getName(), uuid);
        }
        return changeEvent;
    }

    JsonNode removeDroppedColumns(JsonNode changeEvent, String tableId) {
        Map<String, SpannerColumnDefinition> spCols = schema.getSpSchema().get(tableId).getColDefs();
        SourceTable srcTable = schema.getSrcSchema().get(tableId);
        Map<String, SourceColumnDefinition> srcCols = srcTable.getColDefs();
        for (String colId : srcTable.getColIds()) {
            // If spanner columns do not contain this column Id, drop from change event.
            if (!spCols.containsKey(colId)) {
                ((ObjectNode) changeEvent).remove(srcCols.get(colId).getName());
            }
        }
        return changeEvent;
    }

    /**
     * This function changes the modifies and data of the change event. Currently, only supports a
     * single transformation set by roundJsonDecimals.
     */
    public JsonNode transformChangeEventData(JsonNode changeEvent, DatabaseClient dbClient, Ddl ddl)
            throws Exception {
        if (!roundJsonDecimals) {
            return changeEvent;
        }
        String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
        if (ddl.table(tableName) == null) {
            throw new Exception("Table from change event does not exist in Spanner. table=" + tableName);
        }
        Iterator<String> fieldNames = changeEvent.fieldNames();
        List<String> columnNames =
                StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(fieldNames, Spliterator.ORDERED), false)
                        .filter(f -> !f.startsWith(EVENT_METADATA_KEY_PREFIX))
                        .collect(Collectors.toList());
        for (String columnName : columnNames) {
            Type columnType = ddl.table(tableName).column(columnName).type();
            if (columnType.getCode() == Type.Code.JSON || columnType.getCode() == Type.Code.PG_JSONB) {
                // JSON type cannot be a key column, hence setting requiredField to false.
                String jsonStr =
                        ChangeEventTypeConvertor.toString(
                                changeEvent, columnName.toLowerCase(), /* requiredField= */ false);
                if (jsonStr != null) {
                    Statement statement =
                            Statement.newBuilder(
                                            "SELECT PARSE_JSON(@jsonStr, wide_number_mode=>'round') as newJson")
                                    .bind("jsonStr")
                                    .to(jsonStr)
                                    .build();
                    ResultSet resultSet = dbClient.singleUse().executeQuery(statement);
                    while (resultSet.next()) {
                        // We want to send the errors to the severe error queue, hence we do not catch any error
                        // here.
                        String val = resultSet.getJson("newJson");
                        ((ObjectNode) changeEvent).put(columnName.toLowerCase(), val);
                    }
                }
            }
        }
        return changeEvent;
    }
}
