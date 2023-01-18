object snowflakeCRUD{

def main(args: Array[String]){

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

try {
println("Batch Job Started:")
val conf = new SparkConf().setAppName("sf-spark").setMaster("local[4]").set("spark.network.timeout","600s")
val sc = new SparkContext(conf)
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

import sqlContext.implicits._
import java.sql.Date
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import net.snowflake.spark.snowflake.Utils

val sfSource="net.snowflake.spark.snowflake"

//Setting configurations
val sfOptions = Map(
    "sfURL" -> "https://<orgid-acoount_id>.snowflakecomputing.com",
    "sfAccount" -> "<account_id>",
    "sfUser" -> "<account_user>",
    "sfPassword" -> "<account_password>",
    "sfRole" -> "<role>",
    "sfWarehouse" -> "COMPUTE_WH",
    "sfDatabase" -> "SF_SPARK_EXAMPLE",
    "sfSchema" -> "DATA")
	

//Read Customers from Snowflake Table
val customerDf: DataFrame = sqlContext.read.format(sfSource).options(sfOptions).option("dbtable","CUSTOMER").load()

println("Reading Customer Details::")

//Read Orders from Snowflake Table
val orderDf: DataFrame = sqlContext.read.format(sfSource).options(sfOptions).option("dbtable","ORDERS").load()

println("Reading Order Details::")

//Filter Failed Orders on Urgent Priority for customers in autombile segment

val priorityOrders= orderDf.join(customerDf,col("O_CUSTKEY")===col("C_CUSTKEY"),"inner").where("o_orderpriority ='1-URGENT' and o_orderstatus='F' and C_MKTSEGMENT in ('AUTOMOBILE','FURNITURE')").withColumn("o_orderstatus",lit("O")).distinct

println("Filtered and processed Priority orders::")


// writing the identified Prioritized Orders to priority orders table
 priorityOrders.write
    .format("snowflake")
    .options(sfOptions)
    .option("dbtable", "PRIORITY_ORDERS")
    .mode(SaveMode.Overwrite)
    .save()
	
println("Details Written to Priority Orders Table")
	
//Updating the order Status in the original Orders Table. Based on the SQL orders can be updated or Deleted using below

val sqlQuery = "UPDATE DATA.ORDERS O SET o_orderstatus='O' from  DATA.PRIORITY_ORDERS PO where O.O_ORDERKEY=PO.O_ORDERKEY and O.o_orderpriority ='1-URGENT' and O.o_orderstatus='F'"

Utils.runQuery(sfOptions,sqlQuery)	

println("Order Status Updated::")


}

catch{
case e: Exception =>
println("Exception Occured:"+e.toString())
}

finally{
println("Batch Job Completed:")
}



}

}


