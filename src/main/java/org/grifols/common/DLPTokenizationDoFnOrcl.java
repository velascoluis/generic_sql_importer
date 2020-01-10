package org.grifols.common;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.*;
//import com.oracle.tools.packager.Log;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.grifols.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class DLPTokenizationDoFnOrcl extends DoFn<KV<orclTable, List<DbRow>>, KV<orclTable, DbRow>> {

    private static final Logger LOG = LoggerFactory.getLogger(org.grifols.common.DLPTokenizationDoFnOrcl.class);
    private String projectId;
    private DlpServiceClient dlpServiceClient;
    private static final String EMPTY_STRING = "";

    public DLPTokenizationDoFnOrcl(String projectId) {
        this.projectId = projectId;
        dlpServiceClient = null;

    }

    @StartBundle
    public void startBundle() throws SQLException {

        try {
            this.dlpServiceClient = DlpServiceClient.create();
            LOG.info("DLP init");
        } catch (IOException e) {

            LOG.error("***ERROR*** {} Unable to create DLP service ", e.toString());
            throw new RuntimeException(e);
        }

    }

    @FinishBundle
    public void finishBundle() throws Exception {
        if (this.dlpServiceClient != null) {
            this.dlpServiceClient.close();
        }
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws GeneralSecurityException {
        KV<orclTable, List<DbRow>> nonTokenizedData = c.element();
        orclTable tableId = nonTokenizedData.getKey();
        List<DbRow> rows = nonTokenizedData.getValue();
        DLPProperties prop = tableId.getDlpConfig();

        if (prop != null) {
            LOG.info("DEID Template:" + prop.getDeidTemplate());
            LOG.info("INSP Template:" + prop.getInspTemplate());
            List<Table.Row> dlpRows = new ArrayList<>();
            rows.forEach(row -> {
                List<Object> fields = row.fields();
                Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
                for (int i = 0; i < fields.size(); i++) {
                    Object fieldData = fields.get(i);
                    if (fieldData != null) {
                        tableRowBuilder.addValues(Value.newBuilder().setStringValue(fieldData.toString()).build());
                    } else {
                        // null value fix
                        tableRowBuilder.addValues(Value.newBuilder().setStringValue(EMPTY_STRING).build());

                    }
                }
                dlpRows.add(tableRowBuilder.build());

            });

            try {
                Table table = Table.newBuilder().addAllHeaders(ServerUtil.getHeadersOrcl(tableId.getColumnList()))
                        .addAllRows(dlpRows).build();
                LOG.debug("Header Rows: " + tableId.getColumnList());
                dlpRows.clear();
                ContentItem tableItem = ContentItem.newBuilder().setTable(table).build();
                DeidentifyContentRequest request;
                DeidentifyContentResponse response;

                if (prop.getInspTemplate() != null) {
                    request = DeidentifyContentRequest.newBuilder().setParent(ProjectName.of(this.projectId).toString())
                            .setDeidentifyTemplateName(prop.getDeidTemplate())
                            .setInspectTemplateName(prop.getInspTemplate()).setItem(tableItem).build();

                } else {
                    //Solo DEID
                    request = DeidentifyContentRequest.newBuilder().setParent(ProjectName.of(this.projectId).toString())
                            .setDeidentifyTemplateName(prop.getDeidTemplate()).setItem(tableItem).build();

                }


                LOG.debug("DEID request: " + request.toString());
                response = dlpServiceClient.deidentifyContent(request);
                LOG.debug("DEID response: " + response.toString());
                Table encryptedData = response.getItem().getTable();

                LOG.info("Request Size Successfully Tokenized: " + request.toByteString().size() + " bytes."
                        + " Number of rows tokenized: " + response.getItem().getTable().getRowsCount());

                List<Table.Row> outputRows = encryptedData.getRowsList();
                List<Value> values = new ArrayList<Value>();
                List<String> encrytedValues = new ArrayList<String>();

                for (Table.Row outputRow : outputRows) {

                    values = outputRow.getValuesList();

                    values.forEach(value -> {

                        encrytedValues.add(value.getStringValue());

                    });

                    List<Object> objectList = new ArrayList<Object>(encrytedValues);
                    DbRow row = DbRow.create(objectList);
                    LOG.debug("ENCR" + row.toString());
                    c.output(KV.of(tableId, row));
                    encrytedValues.clear();

                }

            } catch (Exception e) {
                LOG.error("***ERROR*** {} Unable to process DLP tokenization request", e.toString());
                throw new RuntimeException(e);
            }

        } else {
            rows.forEach(row -> {
                LOG.debug("WARNING: Prop is null!");
                c.output(KV.of(tableId, row));
            });
        }
    }
}