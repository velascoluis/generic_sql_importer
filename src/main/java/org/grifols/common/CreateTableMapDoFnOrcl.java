package org.grifols.common;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class CreateTableMapDoFnOrcl extends DoFn<ValueProvider<String>, orclTable> {

    public static TupleTag<orclTable> successTag = new TupleTag<orclTable>() {
    };
    public static TupleTag<String> deadLetterTag = new TupleTag<String>() {
    };
    private static final Logger LOG = LoggerFactory.getLogger(CreateTableMapDoFnOrcl.class);

    private ValueProvider<String> excludedTables;
    private ValueProvider<String> dlpConfigBucket;
    private ValueProvider<String> dlpConfigObject;
    private ValueProvider<String> jdbcSpec;
    private ValueProvider<String> jdbcUser;
    private ValueProvider<String> jdbcPass;
    private ValueProvider<String> dataset;
    private String projectId;

    private Connection connection = null;

    public CreateTableMapDoFnOrcl(ValueProvider<String> excludedTables, ValueProvider<String> dlpConfigBucket,
                                 ValueProvider<String> dlpConfigObject, ValueProvider<String> jdbcSpec, ValueProvider<String> jdbcUser, ValueProvider<String> jdbcPass,  ValueProvider<String> dataset,
                                 String projectId) {

        this.excludedTables = excludedTables;
        this.dlpConfigBucket = dlpConfigBucket;
        this.dlpConfigObject = dlpConfigObject;
        this.jdbcSpec = jdbcSpec;
        this.jdbcUser = jdbcUser;
        this.jdbcPass = jdbcPass;
        this.dataset = dataset;
        this.connection = null;
        this.projectId = projectId;

    }

    @Setup
    public void setup() throws InterruptedException, IOException {
        if (this.dataset.isAccessible() && this.dataset.get() != null) {
            BigqueryClient bqClient = new BigqueryClient("DBImportPipeline");
            //todo check create dispositions on at BQ dataset level
            //bqClient.createNewDataset(projectId, this.dataset.get());
            LOG.info("Dataset {} Created/Checked Successfully For Project {}", this.dataset.get(), projectId);
        }
    }

    @StartBundle
    public void startBundle() {

        if (!this.jdbcSpec.get().equals(String.valueOf("TEST_HOST"))) {
            try {
                this.connection = ServerUtil.getConnection(this.jdbcSpec.get(),this.jdbcUser.get(),this.jdbcPass.get());
            } catch (SQLException e) {
                LOG.error("***ERROR** Unable to connect to Database {}", e.getMessage());
                throw new RuntimeException();
            }
        }

    }

    @FinishBundle
    public void finishBundle() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

        if (this.jdbcSpec.get() != null) {

            try {
                List<orclTable> tables = new ArrayList<>();
                if (!this.jdbcSpec.get().equals(String.valueOf("TEST_HOST"))) {
                    LOG.info("Creating new dlpConfigList with: "+this.dlpConfigBucket+" and "+this.dlpConfigObject);
                    final List<DLPProperties> dlpConfigList = ServerUtil.parseDLPconfig(this.dlpConfigBucket,
                            this.dlpConfigObject);

                    if (this.excludedTables.isAccessible() && this.excludedTables.get() != null) {
                        tables = ServerUtil.getTablesListOrcl(this.connection, this.excludedTables.get(), dlpConfigList);

                    } else {

                        tables = ServerUtil.getTablesListOrcl(this.connection, dlpConfigList);

                    }

                }

                for (orclTable table : tables) {
                    LOG.info("Extracting table schema: {} ", table.getFullName());
                    List<orclColumn> tableColumns = new ArrayList<>();

                    tableColumns = ServerUtil.getColumnsListOrcl(connection, table.getName());

                    table.setColumnList(tableColumns);
                    c.output(successTag, table);
                }

            }

            catch (SQLException e) {
                LOG.error("***ERROR** Unable to process table map request {}", e.getMessage());
                c.output(deadLetterTag, e.toString());

            }
        }



    }
}