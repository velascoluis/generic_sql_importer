package org.grifols.common;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.api.client.json.Json;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.privacy.dlp.v2.FieldId;

public class ServerUtil {

    public static final Logger LOG = LoggerFactory.getLogger(ServerUtil.class);
    private static final String COLUMN_NAME_REGEXP = "^[A-Za-z_]+[A-Za-z_0-9]*$";
    private static final Pattern COLUMN_NAME_REGEXP_PATTERN = Pattern.compile(COLUMN_NAME_REGEXP);


    //Oracle dictionay access
    private static final String QUERY_TABLES_ORCL = "SELECT USER AS TABLE_SCHEMA, TABLE_NAME FROM DUAL, USER_TABLES";
    private static final String QUERY_COLUMNS_ORCL = "SELECT COLUMN_NAME,NULLABLE, DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME=?";
    private static final String QUERY_PRIMARY_KEY_ORCL = "SELECT cols.column_name AS COLUMN_NAME FROM user_constraints cons, user_cons_columns cols WHERE cols.table_name = ? AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name";
    public static final Map<orclDataType, String> OrclToBqTypeMap = ImmutableMap.<orclDataType, String>builder()
            .put(orclDataType.BYTE, "STRING")
            .put(orclDataType.CHAR, "STRING")
            .put(orclDataType.NCHAR, "STRING")
            .put(orclDataType.VARCHAR2, "STRING")
            .put(orclDataType.NVARCHAR2, "STRING")
            .put(orclDataType.NUMBER, "FLOAT")
            .put(orclDataType.FLOAT, "FLOAT")
            .put(orclDataType.LONG, "FLOAT")
            .put(orclDataType.DATE, "DATE")
            .put(orclDataType.BINARY_FLOAT, "BYTES")
            .put(orclDataType.BINARY_DOUBLE, "BYTES")
            .put(orclDataType.TIMESTAMP, "TIMESTAMP")
            .put(orclDataType.RAW, "BYTES")
            .put(orclDataType.LONG_RAW, "BYTES")
            .put(orclDataType.ROWID, "STRING")
            .put(orclDataType.UROWID, "STRING")
            .put(orclDataType.CLOB, "BYES")
            .put(orclDataType.NCLOB, "BYES")
            .put(orclDataType.BLOB, "BYES")
            .put(orclDataType.BFILE, "BYES").build();


    public static Connection getConnection(String connectionUrl, String username, String password) throws SQLException {

        Connection connection = null;

        /*
         *Need to provide ojdbcX in lib folder as per gradle.build
         */

        connection = DriverManager.getConnection(connectionUrl, username, password);
        LOG.info("Connection established to"+connectionUrl+"with username:"+username+" and with password:"+password);
        return connection;

    }



    public static List<orclTable> getTablesListOrcl(Connection connection, List<DLPProperties> dlpConfigList)
            throws SQLException {
        List<orclTable> tables = new ArrayList<orclTable>();
        long key = 1;

        if (connection != null) {
            Statement statement = connection.createStatement();
            LOG.info("About to execute statement: "+QUERY_TABLES_ORCL);
            ResultSet rs = statement.executeQuery(QUERY_TABLES_ORCL);
            while (rs.next()) {
                LOG.info("Processing table: Schema is "+rs.getString("TABLE_SCHEMA"));
                LOG.info("Processing table: Tables is "+rs.getString("TABLE_NAME"));
                LOG.info("DLPConfig list "+dlpConfigList.toString());
                orclTable table = new orclTable(key);
                table.setSchema(rs.getString("TABLE_SCHEMA"));
                table.setName(rs.getString("TABLE_NAME"));
                table.setDlpConfig(ServerUtil.extractDLPConfig(table.getName(), dlpConfigList));
                tables.add(table);
                key = key + 1;

            }
        }
        return tables;
    }





    public static List<orclTable> getTablesListOrcl(Connection connection, String excludedTables,
                                                  List<DLPProperties> dlpConfigList) throws SQLException {
        List<orclTable> tables = ServerUtil.getTablesListOrcl(connection, dlpConfigList);
        String[] excludedTableList = ServerUtil.parseExcludedTables(excludedTables);
        tables.removeIf(table -> Arrays.asList(excludedTableList).contains(table.getName()));
        LOG.debug("Excluded Table size: {}", tables.size());
        return tables;
    }



    public static int getRowCount(Connection connection, String schemaName, String tableName) {

        int count = 0;

        String query = String.format("SELECT COUNT(*) as NUMBER_OF_ROWS FROM %s", tableName);
        LOG.debug("query: {}", query);
        try {

            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {

                count = rs.getInt("NUMBER_OF_ROWS");
            }

        } catch (Exception e) {
            LOG.error("***ERROR*** {} Unable to get row count for query {}", e.toString(), query);
        }
        LOG.info("numRows count"+Integer.toString(count));
        return count;

    }





    public static String getPrimaryColumnOrcl(Connection connection, String tableName) {

        String primaryColumnName = null;

        try {
            PreparedStatement statement = connection.prepareStatement(QUERY_PRIMARY_KEY_ORCL);
            LOG.info("About to execute statement: "+QUERY_PRIMARY_KEY_ORCL+"with parameter: "+tableName);
            statement.setString(1, tableName);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                LOG.info("Processing column: Column is "+rs.getString("COLUMN_NAME"));
                primaryColumnName = rs.getString("COLUMN_NAME");
            }

        }

        catch (Exception e) {
            LOG.error("***ERROR*** {} Unable to get primary column connection {}", e.toString(), QUERY_PRIMARY_KEY_ORCL);
        }
        return primaryColumnName;
    }





    public static List<orclColumn> getColumnsListOrcl(Connection connection, String tableName) throws SQLException {
        List<orclColumn> columns = new ArrayList<orclColumn>();
        String primaryKey = ServerUtil.getPrimaryColumnOrcl(connection, tableName);
        PreparedStatement statement = connection.prepareStatement(QUERY_COLUMNS_ORCL);
        statement.setString(1, tableName);
        LOG.info("About to execute statement: "+QUERY_COLUMNS_ORCL+"with value:"+ tableName);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            orclColumn column = new orclColumn();
            column.setName(rs.getString("COLUMN_NAME"));
            if (column.getName().equalsIgnoreCase(primaryKey)) {
                column.setPrimaryKey(true);
            } else {
                column.setPrimaryKey(false);
            }
            //column.setDefaultValue(rs.getString("COLUMN_DEFAULT"));
            //No support for boolean in Oracle, it gives Y or N as a STRING
            boolean nullable = false;
            String nullable_result_string = rs.getString("NULLABLE");
            if ( nullable_result_string.equals("Y"))
                nullable = true;
            column.setNullable(nullable);
            column.setDataType(rs.getString("DATA_TYPE"));
            columns.add(column);

        }

        return columns;
    }




    public static TableSchema getBigQuerySchemaFromOrcl(List<orclColumn> tableColumns) {
        if (tableColumns == null)
            return null;

        TableSchema schema = new TableSchema();
        List<TableFieldSchema> fieldSchemas = new ArrayList<TableFieldSchema>();
        for (orclColumn column : tableColumns) {
            TableFieldSchema fieldSchema = new TableFieldSchema();
            fieldSchema.setName(checkHeaderName(column.getName()));
            String dataTypeString = column.getDataType().toUpperCase();
            orclDataType dataType;
            try {
                dataType = orclDataType.valueOf(dataTypeString);
            } catch (Exception e) {
                LOG.error(String.format(" ***ERROR*** Unrecognized data type %s", dataTypeString));
                throw new RuntimeException(e);
            }
            if (ServerUtil.OrclToBqTypeMap.containsKey(dataType)) {
                fieldSchema.setType(ServerUtil.OrclToBqTypeMap.get(dataType));
            } else {
                LOG.error(String.format("***ERROR*** DataType %s not supported!", dataType));
            }
            fieldSchemas.add(fieldSchema);
        }
        schema.setFields(fieldSchemas);

        return schema;
    }



    public static String findPrimaryKeyOrcl(List<orclColumn> columnNames) {

        // setting up first column as PK if there is no PK set
        orclColumn columnName = columnNames.stream().filter(column -> column.getPrimaryKey()).findFirst()
                .orElse(columnNames.get(0));

        LOG.debug("PRIMARY KEY {}", columnName.getName());

        return columnName.getName();

    }

    public static String[] parseExcludedTables(String excludedTables) {
        return excludedTables.split("-");

    }

    public static List<DLPProperties> parseDLPconfig(ValueProvider<String> bucket, ValueProvider<String> blob) {

        DLPProperties[] configMaps = null;
        if ((!bucket.isAccessible() || !blob.isAccessible())) {
            LOG.info("Bucket or JSON DLP Config file is not accessible");
            return null;
        }
        if (bucket.get() != null && blob.get() != null) {

            Storage storage = StorageOptions.getDefaultInstance().getService();
            BlobId blobId = BlobId.of(bucket.get(), blob.get());

            byte[] bytes = storage.readAllBytes(blobId);
            configMaps = new Gson().fromJson(new String(bytes, StandardCharsets.UTF_8), DLPProperties[].class);


        }

        List<DLPProperties> dlpConfigs = Arrays.asList(Optional.ofNullable(configMaps).orElse(new DLPProperties[0]));

        return dlpConfigs;

    }

    public static DLPProperties extractDLPConfig(String tableId, List<DLPProperties> dlpConfigList) {

        DLPProperties prop = null;
        if (dlpConfigList != null) {
            LOG.info("Extracting DLP config for table : "+tableId);
            //Oracle change, tables are in upper case, need to change to equalsIgnoreCase
            prop = dlpConfigList.stream().filter(config -> config.getTableName().equalsIgnoreCase(tableId)).findFirst()
                    .orElse(null);
        }

        return prop;
    }



    public static List<FieldId> getHeadersOrcl(List<orclColumn> tableHeaders) {

        String[] columnNames = new String[tableHeaders.size()];

        for (int i = 0; i < columnNames.length; i++) {
            columnNames[i] = tableHeaders.get(i).getName();

        }
        List<FieldId> headers = Arrays.stream(columnNames).map(header -> FieldId.newBuilder().setName(header).build())
                .collect(Collectors.toList());
        LOG.debug("HEADERS SIZE {}", headers.size());
        return headers;
    }

    public static String checkHeaderName(String name) {
        // some checks to make sure BQ column names don't fail e.g. special characters
        String checkedHeader = name.replaceAll("\\s", "_");
        checkedHeader = checkedHeader.replaceAll("'", "");
        checkedHeader = checkedHeader.replaceAll("/", "");
        Matcher match = COLUMN_NAME_REGEXP_PATTERN.matcher(checkedHeader);
        if (!match.matches()) {
            throw new IllegalArgumentException("Column name can't be matched to a valid format " + name);
        }
        return checkedHeader;
    }

}