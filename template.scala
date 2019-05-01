



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext


val proj_dir = "C:/Users/Mohamed Ibrahim/Box Sync/eti/"

val data_inp_dir = proj_dir + "02_data/input/"
val data_output_dir = proj_dir + "02_data/output/"
val data_intermediate_dir = proj_dir + "02_data/intermediate/"


val spark = SparkSession.builder().master("local[*]").getOrCreate()
val sqlContext = new SQLContext(spark.sparkContext)
//val sqlContext = new SQLContext(sc)


val inpfil = data_inp_dir + "tst_dump.csv"
val dt = sqlContext.read.option("header","true").option("inferSchema","true").csv(inpfil)

sqlContext.sql("set spark.sql.shuffle.partitions =10")


dt.createOrReplaceTempView("a")


val dt = spark.sql("""
  SELECT TO_DATE(CAST(UNIX_TIMESTAMP(period_id_grants, 'MM/dd/yyyy') AS TIMESTAMP)) period_id_grants,
  TO_DATE(CAST(UNIX_TIMESTAMP(period_id_011, 'MM/dd/yyyy') AS TIMESTAMP)) period_id_011,
  TO_DATE(CAST(UNIX_TIMESTAMP(period_id_campaign, 'MM/dd/yyyy') AS TIMESTAMP)) period_id_campaign,
  TO_DATE(CAST(UNIX_TIMESTAMP(period_id_recharge, 'MM/dd/yyyy') AS TIMESTAMP)) period_id_recharge,
  cast(unix_timestamp(recharge_time, 'MM/dd/yyyy HH:mm:ss') as TIMESTAMP) recharge_time,
  cast(unix_timestamp(sms_delivery_time, 'MM/dd/yyyy HH:mm:ss') as TIMESTAMP) sms_delivery_time,
  cast(unix_timestamp(request_time_011, 'MM/dd/yyyy HH:mm:ss') as TIMESTAMP) request_time_011,
  cast(sub_id_campaign as string) sub_id_campaign,
  cast(sub_id_grants as string) sub_id_grants,
  cast(sub_id_011 as string) sub_id_011,
  cast(sub_id_recharge as string) sub_id_recharge,
  cast(sms_delivery_flag as boolean) sms_delivery_flag,
  cast(total_recharge_amount as double) total_recharge_amount,
  request_type_011,
  granted_gifts
  from a where recharge_time is not null and recharge_time!='?' """
)


val dt2 = dt.withColumn("sms_delivery_flag_1",$"sms_delivery_flag".cast("integer"))

dt2.filter("sms_delivery_flag is not null").select("period_id_grants","sms_delivery_flag","sms_delivery_flag_1").show(10)
