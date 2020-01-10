//todo meter ssn - DONE
//todo datacatalog PII - done
//todo offset - done
//todo hacer dataset mas pequeño
//todo date & timestamp
package org.grifols;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.grifols.common.TableToDbRowFnOrcl;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import org.grifols.common.BigQueryTableDestinationFromOrcl;
import org.grifols.common.BigQueryTableRowDoFnOrcl;
import org.grifols.common.CreateTableMapDoFnOrcl;
import org.grifols.common.DBImportPipelineOptions;
import org.grifols.common.DLPTokenizationDoFnOrcl;
import org.grifols.common.DeterministicKeyCoderOrcl;
import org.grifols.common.orclTable;
import org.grifols.common.TableToDbRowFnOrcl;

public class DBImportPipeline {
    public static final Logger LOG = LoggerFactory.getLogger(DBImportPipeline.class);

    public static void main(String[] args) throws IOException, GeneralSecurityException {

        DBImportPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(DBImportPipelineOptions.class);

        runDBImport(options);

    }

    @SuppressWarnings("serial")
    public static void runDBImport(DBImportPipelineOptions options) {

        Pipeline p = Pipeline.create(options);

        p.getCoderRegistry()
                .registerCoderProvider(CoderProviders.fromStaticMethods(orclTable.class, DeterministicKeyCoderOrcl.class));

        PCollection<ValueProvider<String>> jdbcString = p.apply("Comprueba conexión Oracle",
                Create.of(options.getJDBCSpec()));

        PCollectionTuple tableCollection = jdbcString.apply("Crea mapa de tablas Oracle",
                ParDo.of(new CreateTableMapDoFnOrcl(options.getExcludedTables(), options.getDLPConfigBucket(),
                        options.getDLPConfigObject(), options.getJDBCSpec(),options.getDBUser(),options.getDBPassword() ,options.getDataSet(),
                        options.as(GcpOptions.class).getProject())).withOutputTags(CreateTableMapDoFnOrcl.successTag,
                        TupleTagList.of(CreateTableMapDoFnOrcl.deadLetterTag)));

        PCollectionTuple dbRowKeyValue = tableCollection.get(CreateTableMapDoFnOrcl.successTag).apply("Crea fila tabla",
                ParDo.of(new TableToDbRowFnOrcl(options.getJDBCSpec(),options.getDBUser() ,options.getDBPassword(),options.getOffsetCount()))
                        .withOutputTags(TableToDbRowFnOrcl.successTag, TupleTagList.of(TableToDbRowFnOrcl.deadLetterTag)));

        PCollection<KV<orclTable, TableRow>> successRecords = dbRowKeyValue.get(TableToDbRowFnOrcl.successTag)
                .apply("DLP Tokenization", ParDo.of(new DLPTokenizationDoFnOrcl(options.as(GcpOptions.class).getProject())))
                .apply("Conversion a fila BigQuery", ParDo.of(new BigQueryTableRowDoFnOrcl()))
                .apply(Window.<KV<orclTable, TableRow>>into(FixedWindows.of(Duration.standardSeconds(30)))
                        .triggering(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.ZERO))
                        .discardingFiredPanes().withAllowedLateness(Duration.ZERO));

        WriteResult writeResult = successRecords.apply("Escribe en BigQuery",
                BigQueryIO.<KV<orclTable, TableRow>>write().to(new BigQueryTableDestinationFromOrcl(options.getDataSet()))
                        .withFormatFunction(new SerializableFunction<KV<orclTable, TableRow>, TableRow>() {

                            @Override
                            public TableRow apply(KV<orclTable, TableRow> kv) {
                                return kv.getValue();

                            }
                        }).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withMethod(BigQueryIO.Write.Method.FILE_LOADS));
                        //.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

        writeResult.getFailedInserts().apply("LOG Inserts fallidos en BQ", ParDo.of(new DoFn<TableRow, TableRow>() {

            @ProcessElement
            public void processElement(ProcessContext c) {
                LOG.error("***ERROR*** FAILED INSERT {}", c.element().toString());
                c.output(c.element());
            }

        }));

        PCollectionList
                .of(ImmutableList.of(tableCollection.get(CreateTableMapDoFnOrcl.deadLetterTag),
                        dbRowKeyValue.get(TableToDbRowFnOrcl.deadLetterTag)))
                .apply("Flatten", Flatten.pCollections())
                .apply("Escribe Log Errors", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.error("***ERROR*** DEAD LETTER TAG {}", c.element().toString());
                        c.output(c.element());
                    }
                }));

        p.run();
    }

}