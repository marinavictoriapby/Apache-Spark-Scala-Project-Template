def generateLevelQuery(levels: Seq[String], queryString: String): Seq[String]={
  val Splitted_Levels = levels.mkString.split(":").zip(Stream from 1).sortBy(_._2)
  val Generated_query= for (elem <- Splitted_Levels) yield{
    val rs = for (i <- elem._1.toString.split(",").toList)
      yield " AND l." + i.toString + "=m." + i.toString
    queryString.concat(rs.mkString).replace("cols", elem._1.toString()).replace("orderno", elem._2.toString).replace("AGG_TABLE", "TOP_TABLE")
  }
  Generated_query.toList
}
  val levels = "FCC,BRK:FCC,NAT_CD:FCC,TRA_CD:FCC,NAT,GEO"
  val query = "select orderno as ordno,m.*" +
    ",case when l.CUR_RX_SUM/l.AVG_RX_SUM > 5 " +
    "then 5 when l.CUR_RX_SUM/l.AVG_RX_SUM < 0.0 then 0 when l.CUR_RX_SUM/l.AVG_RX_SUM <= 5 " +
    "OR l.CUR_RX_SUM/l.AVG_RX_SUM >= 0.0 " +
    "THEN l.CUR_RX_SUM/l.AVG_RX_SUM ELSE 0.0 " +
    "END AS growth,l.count_PHCY_ID,l.CUR_RX_SUM,l.AVG_RX_SUM" +
    ",(m.WEEK1 +m.WEEK2 + m.WEEK3+m.WEEK4)/4 FAILED_WEEK_SUM " +
    "from(select cols,count( distinct PHCY_ID) count_PHCY_ID " +
    ",sum(WEEK1 + WEEK2 + WEEK3+WEEK4)/4 AVG_RX_SUM,sum(WEEK5) " +
    "CUR_RX_SUM from AGG_TABLE group by cols) as l,AGG_TABLE as m " +
    "where l.classification=m.classification AND m.IsQCPass=0"
  val sed = levels.split(":").zip(Stream from 1).sortBy(_._2)
  val tact= for (elem <- sed) yield{
    val rs = for (i <- elem._1.toString.split(",").toList)
      yield " AND l." + i.toString + "=m." + i.toString
    query.concat(rs.mkString).replace("cols", elem._1.toString()).replace("orderno", elem._2.toString).replace("AGG_TABLE", "TOP_TABLE")
  }

// Proposal: composing the query
/**
  * Proposal: composing the query per parts
  * Scenario: FCC & BRK, we will compose the first query
  */

val groups = Array(Array("FCC","BRK"),Array("FCC","NAT_CD"),Array("FCC","TRA_CD"),Array("FCC","NAT","GEO"))
val mainQuery = "SELECT N as ordno,m.*,CASE when l.CUR_RX_SUM/l.AVG_RX_SUM > 5 " +
  "then 5 when l.CUR_RX_SUM/l.AVG_RX_SUM < 0.0 then 0 when l.CUR_RX_SUM/l.AVG_RX_SUM <= 5 " +
  "OR l.CUR_RX_SUM/l.AVG_RX_SUM >= 0.0 THEN l.CUR_RX_SUM/l.AVG_RX_SUM ELSE 0.0 END AS growth," +
  "l.count_PHCY_ID,l.CUR_RX_SUM,l.AVG_RX_SUM,(m.WEEK1 +m.WEEK2 + m.WEEK3+m.WEEK4)/4 FAILED_WEEK_SUM FROM"







val fccBrk = Array("FCC","BRK")
val selectClause = "(select $,count(distinct PHCY_ID) count_PHCY_ID ," +
  "sum(WEEK1 + WEEK2 + WEEK3+WEEK4)/4 AVG_RX_SUM,sum(WEEK5) CUR_RX_SUM from TOP_TABLE " +
  "group by $) AS l,TOP_TABLE as m where l.classification=m.classification AND m.IsQCPass=0"
selectClause.flatMap{case '$' => fccBrk.reduce(_+','+_) case c => s"$c"}
//AND l.FCC=m.FCC AND l.BRK=m.BRK
val lastCriteria = "AND l.$=m.$"
fccBrk.map(elem => lastCriteria.flatMap{case '$' => elem case c => s"$c"})

