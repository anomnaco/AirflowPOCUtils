import com.kronos.kpifrm.vacuuming.cassandra.config.{CleanUpConfig, YamlConfig}
import com.kronos.kpifrm.vacuuming.cassandra.Connectors

val connector = Connectors.purgeConnector(spark.sparkContext.getConf)
val yamlConfig = YamlConfig.loadFrom(spark.conf.get("spark.kronos.config"))
val cleanUpConfig = yamlConfig match {case YamlConfig(cleanUpConfig: CleanUpConfig) => cleanUpConfig}

val allConditions = cleanUpConfig.filterConditions
val colSeq = Seq("count(1)","Table")
var tenantsDF = Seq.empty[(Int,String)].toDF(colSeq:_*)
var condition = allConditions(0)

for( condition <- allConditions ) {
  val tenantId = condition.tenantId
  val rowDF = spark.sql("SELECT count(*) FROM test.data_by_date WHERE tenant_uuid='"+tenantId+"'").withColumn("Table", lit("data_by_date"))
  tenantsDF = tenantsDF.union(rowDF)
}

tenantsDF.show(30,false)

Thread.sleep(2000)
spark.stop()
sys.exit(0)