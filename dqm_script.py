# yet to impliment best-practices and error-handeling 
import uuid
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

# Criticality levels
SOFT_FAIL = 0
HARD_FAIL = 1 


#psycopg2



#Obtain the table_schema from the table in DB
def get_table_schema(spark: SparkSession, table_name: str):
    full_schema = spark.table(table_name).schema
    filtered_fields = [
        field
        for field in full_schema.fields
        if field.name != "log_id"
        # we ignore log_id as log_id is defined as an auto incremental column in the table
    ]
    return StructType(filtered_fields)



# create sparkSession & batcch_id
def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()
def generate_batch_id() -> int:
    return uuid.uuid4().int & 0x7FFFFFFFFFFFFFFF



# Check the config table
def load_config(spark: SparkSession, dqm_config_table: str) -> DataFrame:
    df = spark.table(dqm_config_table).filter(col("active") == True)
    if df.isEmpty():
        raise RuntimeError(f"No active configs in {dqm_config_table}")
    return df


#load tables
def load_checks(spark: SparkSession, dqm_checks_table: str) -> DataFrame:
    return spark.table(dqm_checks_table)
def load_source(spark: SparkSession, source_table: str) -> DataFrame:
    return spark.table(source_table).dropDuplicates(["record_id"])



def run_single_check(spark, source_df, config_row, checks_df, batch_id, run_timestamp, quarantine_table, passed_table):

    # make placehold df for passed_record_ids and failed_record_ids
    empty_ids = spark.createDataFrame([], StructType([StructField("record_id", StringType(), True)]))
    
    check_id      = config_row["check_id"]
    target_column = config_row["target_column"]
    target_table  = config_row["target_table"]
    criticality   = int(config_row["criticality"])
    threshold     = float(config_row["threshold"]) if config_row["threshold"] is not None else 0.0

    #collect the parameters from the row in dqm_checks table
    check_rows = checks_df.filter(col("check_id") == check_id).collect()
    if not check_rows:
        return None, empty_ids, empty_ids
    
    query_template = check_rows[0]["query_template"] 
    
    start_time = datetime.now()
    source_df.createOrReplaceTempView("source_table")
    
    
    # fill in the query template
    validation_query = query_template.replace("{table_name}", "source_table").replace("{column_name}", target_column)
    

    # run the query   
    failed_record_ids = spark.sql(validation_query).select("record_id").distinct()


    # counts
    total_rows = source_df.count()
    failed_count = failed_record_ids.count()
    passed_count = total_rows - failed_count
    execution_time = (datetime.now() - start_time).total_seconds()

    fail_pct = (failed_count / total_rows * 100) if total_rows > 0 else 0.0
    status = "FAIL" if fail_pct > threshold else "PASS"

# create the record for the log table
    log_record = {
        "batch_id": int(batch_id),
        "check_id": int(check_id),
        "target_table": str(target_table),
        "target_column": str(target_column),
        "total_rows": int(total_rows),
        "passed_rows": int(passed_count),
        "failed_rows": int(failed_count),
        "threshold": float(threshold),
        "status": str(status),
        "criticality": int(criticality),
        "run_timestamp": run_timestamp,
        "execution_time": float(execution_time),
        "quarantine_table": str(quarantine_table),
        "passed_table": str(passed_table),
    }

    # assign hard-fail ids and soft-fail ids
    hard_ids = failed_record_ids if criticality == HARD_FAIL else empty_ids
    soft_ids = failed_record_ids if criticality == SOFT_FAIL else empty_ids
    return log_record, hard_ids, soft_ids

# creates passed_df and quarantined_df according to priority 
def classify_records(spark, source_df, hard_ids, soft_ids):
    pure_soft_ids = soft_ids.subtract(hard_ids)
    all_failed_ids = hard_ids.union(soft_ids).distinct()
    clean_ids = source_df.select("record_id").subtract(all_failed_ids)

    quarantined_df = source_df.join(hard_ids, on="record_id", how="inner").dropDuplicates(["record_id"])
    passed_ids = clean_ids.union(pure_soft_ids).distinct()
    passed_df = source_df.join(passed_ids, on="record_id", how="inner").dropDuplicates(["record_id"])
    return passed_df, quarantined_df


#code to write to respective tables
def persist_results(spark, log_records, dqm_logs_table, passed_df, passed_table, quarantined_df, quarantine_table):
    if log_records:
        log_schema = get_table_schema(spark, dqm_logs_table)
        logs_df = spark.createDataFrame(log_records, schema=log_schema)
        logs_df.write.format("delta").mode("append").saveAsTable(dqm_logs_table)
    
    passed_df.write.format("delta").mode("overwrite").saveAsTable(passed_table)
    quarantined_df.write.format("delta").mode("overwrite").saveAsTable(quarantine_table)

# function to bring it all together

def run_dqm_pipeline(catalog, schema,ctrl_dqm_master,ctrl_dqm_type,source,quarantine_table,passed_table,log_table):
    ns = f"{catalog}.{schema}"
    spark = get_spark()
    batch_id = generate_batch_id()
    run_timestamp = datetime.now()
    
    config_df = load_config(spark, f"{ns}.{ctrl_dqm_master}")
    checks_df = load_checks(spark, f"{ns}.{ctrl_dqm_type}")
    source_df = load_source(spark, f"{ns}.{source}")

    hard_ids = spark.createDataFrame([], StructType([StructField("record_id", StringType(), True)]))
    soft_ids = hard_ids
    log_records = []

    for config_row in config_df.collect():
        log_record, r_hard, r_soft = run_single_check(
            spark, source_df, config_row, checks_df, batch_id, 
            run_timestamp, f"{ns}.{quarantine_table}", f"{ns}.{passed_table}"
        )
        if log_record:
            log_records.append(log_record)
            hard_ids = hard_ids.union(r_hard).distinct()
            soft_ids = soft_ids.union(r_soft).distinct()

    passed_df, quarantined_df = classify_records(spark, source_df, hard_ids, soft_ids)
    
    persist_results(
        spark, log_records, f"{ns}.{log_table}", 
        passed_df, f"{ns}.{passed_table}", 
        quarantined_df, f"{ns}.{quarantine_table}"
    )

    return {"batch_id": batch_id, "passed": passed_df.count(), "quarantined": quarantined_df.count()}

#run
run_dqm_pipeline("com_edp_dev"
,"com_raw"
,"ctrl_dqm_master"
,"ctrl_dqm_type"
,"dqm_staging"
,"dqm_quarantined_records"
,"dqm_passed_records"
,"dqm_log")