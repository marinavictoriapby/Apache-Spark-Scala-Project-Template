
/**
select 1 as ordno,m.*,
case when l.CUR_RX_SUM/l.AVG_RX_SUM > 5 then 5
when l.CUR_RX_SUM/l.AVG_RX_SUM < 0.0 then 0
when l.CUR_RX_SUM/l.AVG_RX_SUM <= 5 OR l.CUR_RX_SUM/l.AVG_RX_SUM >= 0.0
THEN l.CUR_RX_SUM/l.AVG_RX_SUM ELSE 0.0 END AS growth,
l.count_PHCY_ID,
l.CUR_RX_SUM,
l.AVG_RX_SUM,
(m.WEEK1 +m.WEEK2 + m.WEEK3+m.WEEK4)/4 FAILED_WEEK_SUM from
(select FCC,BRK,count( distinct PHCY_ID) count_PHCY_ID ,
sum(WEEK1 + WEEK2 + WEEK3+WEEK4)/4 AVG_RX_SUM,
sum(WEEK5) CUR_RX_SUM from TOP_TABLE group by FCC,BRK) as l,
TOP_TABLE as m where l.classification=m.classification AND m.IsQCPass=0 AND l.FCC=m.FCC AND l.BRK=m.BRK
Union all
select 2 as ordno,m.*,case when l.CUR_RX_SUM/l.AVG_RX_SUM > 5 then 5 when l.CUR_RX_SUM/l.AVG_RX_SUM < 0.0 then 0 when l.CUR_RX_SUM/l.AVG_RX_SUM <= 5 OR l.CUR_RX_SUM/l.AVG_RX_SUM >= 0.0 THEN l.CUR_RX_SUM/l.AVG_RX_SUM ELSE 0.0 END AS growth,l.count_PHCY_ID,l.CUR_RX_SUM,l.AVG_RX_SUM,(m.WEEK1 +m.WEEK2 + m.WEEK3+m.WEEK4)/4 FAILED_WEEK_SUM from(select FCC,NAT_CD,count( distinct PHCY_ID) count_PHCY_ID ,sum(WEEK1 + WEEK2 + WEEK3+WEEK4)/4 AVG_RX_SUM,sum(WEEK5) CUR_RX_SUM from TOP_TABLE group by FCC,NAT_CD) as l,TOP_TABLE as m where l.classification=m.classification AND m.IsQCPass=0 AND l.FCC=m.FCC AND l.NAT_CD=m.NAT_CD
Union all
select 3 as ordno,m.*,case when l.CUR_RX_SUM/l.AVG_RX_SUM > 5 then 5 when l.CUR_RX_SUM/l.AVG_RX_SUM < 0.0 then 0 when l.CUR_RX_SUM/l.AVG_RX_SUM <= 5 OR l.CUR_RX_SUM/l.AVG_RX_SUM >= 0.0 THEN l.CUR_RX_SUM/l.AVG_RX_SUM ELSE 0.0 END AS growth,l.count_PHCY_ID,l.CUR_RX_SUM,l.AVG_RX_SUM,(m.WEEK1 +m.WEEK2 + m.WEEK3+m.WEEK4)/4 FAILED_WEEK_SUM from(select FCC,TRA_CD,count( distinct PHCY_ID) count_PHCY_ID ,sum(WEEK1 + WEEK2 + WEEK3+WEEK4)/4 AVG_RX_SUM,sum(WEEK5) CUR_RX_SUM from TOP_TABLE group by FCC,TRA_CD) as l,TOP_TABLE as m where l.classification=m.classification AND m.IsQCPass=0 AND l.FCC=m.FCC AND l.TRA_CD=m.TRA_CD
Union all
select 4 as ordno,m.*,case when l.CUR_RX_SUM/l.AVG_RX_SUM > 5 then 5 when l.CUR_RX_SUM/l.AVG_RX_SUM < 0.0 then 0 when l.CUR_RX_SUM/l.AVG_RX_SUM <= 5 OR l.CUR_RX_SUM/l.AVG_RX_SUM >= 0.0 THEN l.CUR_RX_SUM/l.AVG_RX_SUM ELSE 0.0 END AS growth,l.count_PHCY_ID,l.CUR_RX_SUM,l.AVG_RX_SUM,(m.WEEK1 +m.WEEK2 + m.WEEK3+m.WEEK4)/4 FAILED_WEEK_SUM from(select FCC,NAT,GEO,count( distinct PHCY_ID) count_PHCY_ID ,sum(WEEK1 + WEEK2 + WEEK3+WEEK4)/4 AVG_RX_SUM,sum(WEEK5) CUR_RX_SUM from TOP_TABLE group by FCC,NAT,GEO) as l,TOP_TABLE as m where l.classification=m.classification AND m.IsQCPass=0 AND l.FCC=m.FCC and l.NAT=m.NAT AND l.GEO=m.GEO
**/

val level1 = "FCC,BRK"
val level1Proc = level1.split(",")



