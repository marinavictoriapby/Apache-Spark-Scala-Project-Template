package net.sparktutorials.scaffold

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.{col, when}

class ExampleClass extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)    

  def cobble(arg:String) = {
    logger.info("Now cobbling!")
    "Cobble the " + arg
  }
}

object ExampleClass extends Serializable {
  def main(args:Array[String]) = {
    val name = "Example Application"
    val conf = new SparkConf().setAppName(name).setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    val mc = new ExampleClass()
    val data = sc.parallelize(Array((1,2,3),(4,5,6)).map(x => Row(x._1, x._2, x._3)))

    val sampledf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("prev_agg_sample.csv")


    val level1 = "FCC,BRK".split(",")

    /**
      * (select FCC,BRK,count( distinct PHCY_ID) count_PHCY_ID ,
        sum(WEEK1 + WEEK2 + WEEK3+WEEK4)/4 AVG_RX_SUM,
        sum(WEEK5) CUR_RX_SUM from TOP_TABLE group by FCC,BRK) as l
      */

    //val select1 = df.select("count_PHCY_ID", level1:_*)

    //val select1 = sampledf.select("fcc","brk","phcy_id").groupBy("fcc","brk").agg(countDistinct("phcy_id").alias("count_phcy_id"))
    /*
              select
          [order] orderNo ,
          m.*,
          case when l.CUR_RX_SUM/l.AVG_RX_SUM &gt; [MaxGrowth] then [MaxGrowth]
          when l.CUR_RX_SUM/l.AVG_RX_SUM &lt; [MinGrowth] then [MinGrowth]
          when l.CUR_RX_SUM/l.AVG_RX_SUM &lt;= [MaxGrowth] OR l.CUR_RX_SUM/l.AVG_RX_SUM &gt;= [MinGrowth] THEN
          l.CUR_RX_SUM/l.AVG_RX_SUM ELSE [MinGrowth] END AS growth,
          l.count_PHCY_ID,
          l.CUR_RX_SUM,
          l.AVG_RX_SUM
          from (
          select classification,[columns],
          count(distinct curr_week_phcy)-1 as count_PHCY_ID,  -- here -1 is done as oneof the disticnt entres will be 0 as well for the phcy which does not have ant Tx in current week
          sum(last_4_sum)/4 as AVG_RX_SUM,
          sum(curr_sum) as CUR_RX_SUM
          from [AGG_TABLE] where
          IsQCPass=1
          group by classification,[columns]
          having count_PHCY_ID &gt;= [NumberOfStores]
          AND
          CUR_RX_SUM &gt;= [NumberOfUnprojectedRx]
          ) as l,
          [GF_WEEKLY_AGG_4] as m where m.IsQCPass=0 and m.classification=l.classification [filter]
     */
    val maxGrowth = 5
    val minGrowth = 0

    //where l.classification=m.classification AND m.IsQCPass=0 AND l.FCC=m.FCC AND l.BRK=m.BRK
    val partitioned = sampledf.repartition(col("classification"),col("fcc"),col("brk"))
    val cached = partitioned.cache()

    sqlContext.udf.register("avg", (a: Integer, b: Integer, c: Integer, d: Integer) => (a + b + c + d) / 4)

    val growthCalculation = cached.filter(col("isqcpass") === 0).withColumn("growth",
      when(col("curr_sum") / col("avg_sum") > maxGrowth, maxGrowth)
        .when(col("curr_sum") / col("avg_sum") < minGrowth, minGrowth)
        .when(col("curr_sum") / col("avg_sum") < maxGrowth or (col("curr_sum") / col("avg_sum") > minGrowth),
          col("curr_sum") / col("avg_sum")).otherwise(minGrowth))



    //growthCalculation.show(10)


    val sampleMini = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("mini.csv")

    sampleMini.registerTempTable("sampleMini")

    sqlContext.sql("select FCC,BRK,count( distinct PHCY_ID) count_PHCY_ID from sampleMini").show


    //val result = log.select("page","visitor").groupBy('page).agg('page, countDistinct('visitor))


    /*
    (select FCC,BRK,count( distinct PHCY_ID) count_PHCY_ID ,
    sum(WEEK1 + WEEK2 + WEEK3+WEEK4)/4 AVG_RX_SUM,
    sum(WEEK5) CUR_RX_SUM from TOP_TABLE group by FCC,BRK) AS l,
     */

    cached.select()
    /*
      select
      growth,
      l.count_PHCY_ID,
      l.CUR_RX_SUM,
      l.AVG_RX_SUM,
      (m.WEEK1 +m.WEEK2 + m.WEEK3+m.WEEK4)/4 FAILED_WEEK_SUM
    */
    val cachedFailedWeeks = cached.withColumn("failed_week_sum", (col("week1") + col("week2") + col("week3") + col("week4")) / 4)
    //firstPart.select(col("growth"), col("count_phcy_id"), col("curr_sum"), col("avg_sum"))


  }
}
