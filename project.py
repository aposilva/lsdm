from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, FloatType, BooleanType
from pyspark.sql.functions import col, corr, avg, countDistinct, desc
# Initialize a SparkSession
spark = SparkSession.builder.appName("TestApp").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

job_events_schema = StructType([
    StructField("time", IntegerType(), nullable=False),  # Mandatory field
    StructField("missing_info", IntegerType(), nullable=True),  # Optional field
    StructField("job_id", LongType(), nullable=False),  # Mandatory field
    StructField("event_type", IntegerType(), nullable=False),  # Mandatory field
    StructField("user", StringType(), nullable=True),  # Optional field
    StructField("scheduling_class", IntegerType(), nullable=True),  # Optional field
    StructField("job_name", StringType(), nullable=True),  # Optional field
    StructField("logical_job_name", StringType(), nullable=True),  # Optional field
])

task_events_schema = StructType([
    StructField("time", IntegerType(), nullable=False),  # Mandatory field
    StructField("missing_info", IntegerType(), nullable=True),  # Optional field
    StructField("job_id", LongType(), nullable=False),  # Mandatory field
    StructField("task_index", IntegerType(), nullable=False),  # Mandatory field
    StructField("machine_id", LongType(), nullable=True),  # Optional field
    StructField("event_type", IntegerType(), nullable=False),  # Mandatory field
    StructField("user", StringType(), nullable=True),  # Optional field
    StructField("scheduling_class", IntegerType(), nullable=True),  # Optional field
    StructField("priority", IntegerType(), nullable=False),  # Mandatory field
    StructField("cpu_request", FloatType(), nullable=True),  # Optional field
    StructField("memory_request", FloatType(), nullable=True),  # Optional field
    StructField("disk_space_request", FloatType(), nullable=True),  # Optional field
    StructField("different_machines_restriction", BooleanType(), nullable=True),  # Optional field
])

machine_events_schema = StructType([
    StructField("time", IntegerType(), nullable=False),  # Mandatory field
    StructField("machine_id", LongType(), nullable=False),  # Mandatory field
    StructField("event_type", IntegerType(), nullable=False),  # Mandatory field
    StructField("platform_id", StringType(), nullable=True),  # Optional field
    StructField("cpus", FloatType(), nullable=True),  # Optional field
    StructField("memory", FloatType(), nullable=True),  # Optional field
])

machine_attributes_schema = StructType([
    StructField("time", IntegerType(), nullable=False),  # Mandatory field
    StructField("machine_id", LongType(), nullable=False),  # Mandatory field
    StructField("attribute_name", StringType(), nullable=False),  # Mandatory field
    StructField("attribute_value", StringType(), nullable=True),  # Optional field
    StructField("attribute_deleted", BooleanType(), nullable=False)  # Mandatory field
])

task_constraints_schema = StructType([
    StructField("time", IntegerType(), nullable=False),  # Mandatory field
    StructField("job_id", LongType(), nullable=False),  # Mandatory field
    StructField("task_index", IntegerType(), nullable=False),  # Mandatory field
    StructField("comparison_operator", StringType(), nullable=False),  # Mandatory field
    StructField("attribute_name", StringType(), nullable=False),  # Mandatory field
    StructField("attribute_value", StringType(), nullable=True),  # Optional field
])

task_usage_schema = StructType([
    StructField("start_time", LongType(), nullable=False),  # Mandatory field
    StructField("end_time", LongType(), nullable=False),  # Mandatory field
    StructField("job_id", LongType(), nullable=False),  # Mandatory field
    StructField("task_index", LongType(), nullable=False),  # Mandatory field
    StructField("machine_id", LongType(), nullable=False),  # Mandatory field
    StructField("cpu_rate", FloatType(), nullable=True),  # Optional field
    StructField("canonical_memory_usage", FloatType(), nullable=True),  # Optional field
    StructField("assigned_memory_usage", FloatType(), nullable=True),  # Optional field
    StructField("unmapped_page_cache", FloatType(), nullable=True),  # Optional field
    StructField("total_page_cache", FloatType(), nullable=True),  # Optional field
    StructField("maximum_memory_usage", FloatType(), nullable=True),  # Optional field
    StructField("disk_io_time", FloatType(), nullable=True),  # Optional field
    StructField("local_disk_space_usage", FloatType(), nullable=True),  # Optional field
    StructField("maximum_cpu_rate", FloatType(), nullable=True),  # Optional field
    StructField("maximum_disk_io_time", FloatType(), nullable=True),  # Optional field
    StructField("cycles_per_instruction", FloatType(), nullable=True),  # Optional field
    StructField("memory_accesses_per_instruction", FloatType(), nullable=True),  # Optional field
    StructField("sample_portion", FloatType(), nullable=True),  # Optional field
    StructField("aggregation_type", BooleanType(), nullable=True),  # Optional field
    StructField("sampled_cpu_usage", FloatType(), nullable=True),  # Optional field
])

job_events_df = spark.read.csv("job_events/part-00000-of-00500.csv", schema=job_events_schema, header=False)

machine_events_df = spark.read.csv("machine_events/part-00000-of-00001.csv", schema=machine_events_schema, header=False)

machine_attributes_df = spark.read.csv("machine_attributes/part-00000-of-00001.csv", schema=machine_attributes_schema, header=False)

task_events_df = spark.read.csv("task_events/part-00000-of-00500.csv", schema=task_events_schema, header=False)

task_usage_df = spark.read.csv("task_usage/part-00000-of-00500.csv", schema=task_usage_schema, header=False)

task_constraints_df = spark.read.csv("task_constraints/part-00000-of-00500.csv", schema=task_constraints_schema, header=False)

print("Q1: What is the distribution of the machines according to their CPU capacity?")
filtered_machine_events_df = machine_events_df.filter(col("cpus").isNotNull())
cpu_distribution = filtered_machine_events_df.groupBy("cpus").count().orderBy("cpus")
cpu_distribution.show()

print("Q2: What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?")
total_cpu_capacity = machine_events_df.selectExpr("sum(cpus)").collect()[0][0]
offline_reconnected_cpu_capacity = machine_events_df.filter(col("event_type") == 2).selectExpr("sum(cpus)").collect()[0][0]
percentage_lost = (offline_reconnected_cpu_capacity / total_cpu_capacity) * 100

print("The percentage of computational power lost due to maintenance is:", percentage_lost)


print("Q3: What is the distribution of the number of jobs/tasks per scheduling class?")
jobs_per_scheduling_class = job_events_df.groupBy("scheduling_class").count().orderBy("scheduling_class")
tasks_per_scheduling_class = task_events_df.groupBy("scheduling_class").count().orderBy("scheduling_class")

print("Jobs per scheduling class:")
jobs_per_scheduling_class.show()
print("Tasks per scheduling class:")
tasks_per_scheduling_class.show()


print("Q4: Do tasks with a low scheduling class have a higher probability of being evicted?")
eviction_events_df = task_events_df.filter(task_events_df["event_type"] == 2)
total_tasks_per_class = task_events_df.groupBy("scheduling_class").count()
evictions_per_class = eviction_events_df.groupBy("scheduling_class").count()
eviction_probability = total_tasks_per_class.alias("total").join(evictions_per_class.alias("evictions"), "scheduling_class") \
    .select(
        col("scheduling_class"),
        (col("evictions.count") / col("total.count")).alias("eviction_probability")
    ).orderBy("scheduling_class")

print("Eviction Probability per Scheduling Class:")
eviction_probability.show()


print("Q5: In general, do tasks from the same job run on the same machine?")
task_usage_df_renamed = task_usage_df.withColumnRenamed("machine_id", "usage_machine_id")
task_events_with_usage_df = task_events_df.join(task_usage_df_renamed, ["job_id", "task_index"])
machines_per_job = task_events_with_usage_df.groupBy("job_id").agg(countDistinct("usage_machine_id").alias("machines_per_job"))
jobs_per_machines = machines_per_job.groupBy("machines_per_job").count().orderBy(desc("count"))

jobs_per_machines.show()


print("Q6: Are the tasks that request the more resources the one that consume the more resources?")
joined_df = task_events_df.join(task_usage_df, ["job_id", "task_index"])
resource_analysis_df = joined_df.groupBy("job_id", "task_index").agg(
    avg("cpu_request").alias("avg_cpu_request"),
    avg("memory_request").alias("avg_memory_request"),
    avg("cpu_rate").alias("avg_cpu_rate"),
    avg("assigned_memory_usage").alias("avg_assigned_memory_usage")
)
correlation_df = resource_analysis_df.select(
    corr("avg_cpu_request", "avg_cpu_rate").alias("cpu_correlation"),
    corr("avg_memory_request", "avg_assigned_memory_usage").alias("memory_correlation")
)

correlation_df.show()


print("Q7: Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?")
task_usage_df_renamed = task_usage_df.withColumnRenamed("machine_id", "usage_machine_id")
eviction_events_df = task_events_df.filter(task_events_df["event_type"] == 2)
joined_df = eviction_events_df.join(task_usage_df_renamed, ["job_id", "task_index"])
resource_analysis_df = joined_df.groupBy("job_id", "task_index").agg(
    avg("cpu_rate").alias("avg_actual_cpu"),
    avg("assigned_memory_usage").alias("avg_actual_memory")
)

machine_analysis_df = joined_df.groupBy("machine_id").agg(
    avg("cpu_rate").alias("avg_actual_cpu"),
    avg("assigned_memory_usage").alias("avg_actual_memory")
)
correlation_df = resource_analysis_df.select(corr("avg_actual_cpu", "avg_actual_memory").alias("task_correlation")).crossJoin(
                  machine_analysis_df.select(corr("avg_actual_cpu", "avg_actual_memory").alias("machine_correlation"))
)

correlation_df.show()


spark.stop()
