package org.grifols.common;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface DBImportPipelineOptions extends PipelineOptions, DataflowPipelineOptions {


    //Listado de variables que pasamos por comando en -Dargs
    //i.e. getJDBCSpec, captura a --JDBCSpec=value
    @Description("JDBC Spec")
    ValueProvider<String> getJDBCSpec();

    void setJDBCSpec(ValueProvider<String> value);
    //Added new parameters username and password as they are not supported directly on the conn string
    @Description("Database username")
    ValueProvider<String> getDBUser();

    void setDBUser(ValueProvider<String> value);

    @Description("Database password")
    ValueProvider<String> getDBPassword();

    void setDBPassword(ValueProvider<String> value);

    @Description("BQ Dataset")
    ValueProvider<String> getDataSet();

    void setDataSet(ValueProvider<String> value);

    @Description("Exclude Tables")
    ValueProvider<String> getExcludedTables();

    void setExcludedTables(ValueProvider<String> value);

    @Description("Table Offset Count")
    ValueProvider<Integer> getOffsetCount();

    void setOffsetCount(ValueProvider<Integer> value);

    @Description("DLP Bucket Config")
    ValueProvider<String> getDLPConfigBucket();

    void setDLPConfigBucket(ValueProvider<String> value);

    @Description("DLP Object Config")
    ValueProvider<String> getDLPConfigObject();

    void setDLPConfigObject(ValueProvider<String> value);
}

