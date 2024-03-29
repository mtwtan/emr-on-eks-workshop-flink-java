package com.amazonaws.emr.flink.workshop;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableResult.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkKinesisKdg {

    private static String createSourceTable(String tableName, String streamName, String region) {
        return String.format("CREATE TABLE %s (" +
                "    myuuid VARCHAR," +
                "    event_time TIMESTAMP(3)," +
                "    name VARCHAR," +
                "    address VARCHAR," +
                "    city VARCHAR," +
                "    zipcode VARCHAR," +
                "    country VARCHAR," +
                "    email VARCHAR," +
                "    phone VARCHAR," +
                "    coffee VARCHAR," +
                "    account VARCHAR," +
                "    currency VARCHAR," +
                "    num_ordered INT," +
                "    ccnum VARCHAR," +
                "    ccexpiry VARCHAR," +
                "    ccsecurecode VARCHAR," +
                "    ip VARCHAR," +
                "    browser VARCHAR," +
                "    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND"
                ") WITH (" +
                "    'connector' = 'kinesis'," +
                "    'stream' = '%s'," +
                "    'aws.region' = '%s'," +
                "    'scan.stream.initpos' = 'LATEST'," +
                "    'format' = 'json'" +
                ")", tableName, streamName, region);
    }

    private static String createSinkTable(String tableName, String filePath) {
        return String.format(
                "CREATE TABLE %s (" +
                "    myuuid VARCHAR," +
                "    event_time TIMESTAMP," +
                "    name VARCHAR," +
                "    address VARCHAR," +
                "    city VARCHAR," +
                "    zipcode VARCHAR," +
                "    country VARCHAR," +
                "    email VARCHAR," +
                "    phone VARCHAR," +
                "    coffee VARCHAR," +
                "    unit_price DOUBLE," +
                "    total_amount DOUBLE," +
                "    account VARCHAR," +
                "    currency VARCHAR," +
                "    num_ordered INT," +
                "    ccnum VARCHAR," +
                "    ccexpiry VARCHAR," +
                "    ccsecurecode VARCHAR," +
                "    ip VARCHAR," +
                "    browser VARCHAR," +
                "    r_year VARCHAR," +
                "    r_month VARCHAR," +
                "    r_day VARCHAR," +
                "    r_hour VARCHAR" +
                ") PARTITIONED BY (r_year, r_month, r_day) WITH (" +
                "    'connector' = 'filesystem'," +
                "    'path' = '%s'," +
                "    'format' = 'parquet'," +
                "    'sink.partition-commit.delay' = '1 h'," +
                "    'sink.partition-commit.policy.kind' = 'success-file'" +
                ")", tableName, filePath);
    }

    private static String createSinkTableTumblingWindow(String tableName, String filePath) {
        return String.format(
                "CREATE TABLE %s (" +
                "    window_start TIMESTAMP," +
                "    window_end TIMESTAMP," +
                "    coffee VARCHAR," +
                "    min_total_amount DOUBLE," +
                "    max_total_amount DOUBLE," +
                "    sum_total_amount DOUBLE," +
                "    stddev_total_amount DOUBLE" +
                ") PARTITIONED BY (coffee) WITH (" +
                "    'connector' = 'filesystem'," +
                "    'path' = '%s'," +
                "    'format' = 'parquet'," +
                "    'sink.partition-commit.delay' = '1 h'," +
                "    'sink.partition-commit.policy.kind' = 'success-file'" +
                ")", tableName, filePath);
    }

    private static String setInsertSqlSinkAllToS3(String source_table_name, String sink_table_name) {
        return String.format(
            "INSERT INTO %s " +
            "SELECT " +
            "myuuid, " +
            "event_time, " +
            "name, " +
            "address, " +
            "city, " +
            "zipcode, " +
            "country, " +
            "email, " +
            "phone, " +
            "coffee, " +
            "CASE " +
            "WHEN (coffee = 'pour') THEN 2.50 " +
            "WHEN (coffee = 'latte') THEN 4.50 " +
            "WHEN (coffee = 'mocha') THEN 5.00 " +
            "ELSE 0.00 " +
            "END as unit_price, " +
            "CASE " +
            "WHEN (coffee = 'pour') THEN 2.50 * num_ordered " +
            "WHEN (coffee = 'latte') THEN 4.50 * num_ordered " +
            "WHEN (coffee = 'mocha') THEN 5.00 * num_ordered " +
            "ELSE 0.00 " +
            "END as total_amount, " +
            "account, " +
            "currency, " +
            "num_ordered, " +
            "ccnum, " +
            "ccexpiry, " +
            "ccsecurecode, " +
            "ip, " +
            "browser, " +
            "cast(year(event_time) AS VARCHAR) as r_year, " +
            "cast(month(event_time) AS VARCHAR) as r_month, " +
            "cast(dayofmonth(event_time) AS VARCHAR) as r_day, " +
            "cast(hour(event_time) AS VARCHAR) as r_hour " +
            "FROM %s", sink_table_name, source_table_name
        );
    }

    private static String setInsertSqlTumblingWindow(String source_table_name, String sink_table_name) {
        return String.format("INSERT INTO %s " + 
            "SELECT window_start, window_end, coffee, " +
            "MIN(CASE " + 
            "  WHEN (coffee = 'pour') THEN 2.50 * num_ordered " + 
            "  WHEN (coffee = 'latte') THEN 4.50 * num_ordered " + 
            "  WHEN (coffee = 'mocha') THEN 5.00 * num_ordered " + 
            "  ELSE 0.00 " + 
            "END ) as min_total_amount, " +
            "MAX(CASE " +
            "  WHEN (coffee = 'pour') THEN 2.50 * num_ordered " +
            "  WHEN (coffee = 'latte') THEN 4.50 * num_ordered " +
            "  WHEN (coffee = 'mocha') THEN 5.00 * num_ordered " +
            "  ELSE 0.00 " +
            "END ) as max_total_amount, " +
            "SUM(CASE " +
            "  WHEN (coffee = 'pour') THEN 2.50 * num_ordered " +
            "  WHEN (coffee = 'latte') THEN 4.50 * num_ordered " +
            "  WHEN (coffee = 'mocha') THEN 5.00 * num_ordered " +
            "  ELSE 0.00 " +
            "END ) as sum_total_amount, " +
            "STDDEV_POP(CASE " +
            "  WHEN (coffee = 'pour') THEN 2.50 * num_ordered " +
            "  WHEN (coffee = 'latte') THEN 4.50 * num_ordered " +
            "  WHEN (coffee = 'mocha') THEN 5.00 * num_ordered " +
            "  ELSE 0.00 " +
            "END ) as stddev_total_amount " +
            "FROM table ( " +
                " tumble(table %s, descriptor(event_time), interval '1' minute) " +
                ") " +
                "group by window_start, window_end, coffee ", sink_table_name, source_table_name);
    }



    // create environments of both APIs
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //env.setRuntimeMode(RuntimeExecutionMode.BATCH);

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String stream_name = args[0];
        String region = args[1];
        String bucket_name = args[2];

        String source_table_name = "source_coffee_stream";
        String sink_table_name = "sink_coffee_stream";
        String sink_file_path = String.format("s3://%s/sink/coffee-stream/", bucket_name);
        String sink_tumbling_window_table_name = "sink_tumbling_window_coffee_stream";
        String sink_tumbling_window_file_path = String.format("s3://%s/sink/coffee-stream-tumbling-windows/", bucket_name);

        // **** create tables

        // Source table

        tableEnv.executeSql(
            createSourceTable(source_table_name, stream_name, region)
        );

        // Sink table

        tableEnv.executeSql(
            createSinkTable(sink_table_name,sink_file_path)
        );
        
        // Tumbling windows table

        tableEnv.executeSql(
            createSinkTableTumblingWindow(sink_tumbling_window_table_name,sink_tumbling_window_file_path)
        );

        // **** Insert streams into tables

        // Multiple insert

        StatementSet stmtSet = tableEnv.createStatementSet();

        // Tumbling windows

        stmtSet.addInsertSql(setInsertSqlTumblingWindow(
            source_table_name,sink_tumbling_window_table_name
        ));


        // Data sink to S3

        stmtSet.addInsertSql(
            setInsertSqlSinkAllToS3(
                source_table_name,sink_table_name
            )
        );

        stmtSet.addInsertSql(
            setInsertSqlSinkAllToS3(
                source_table_name,
                sink_table_name
            )
        );

        TableResult tableResult = stmtSet.execute();

        System.out.println(tableResult.getJobClient().get().getJobStatus());

    }
    
}
