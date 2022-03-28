package org.pucc.flink.cdc

import com.ververica.cdc.connectors.sqlserver.SqlServerSource
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.pucc.flink.cdc.SqlServerConstant._

object SqlServerConstant{
  val SqlServer_ADDRESS = "192.168.0.54"
  val SqlServer_PORT = 1433
  val USER_NAME = "sa"
  val USER_PASSWORD = "cdgl@123"
  val DATABASE="test"
  val TABLE_LIST="dbo.myuser"
}

object SqlServerCDC extends App{

//  assert(args.length == 1)
//  val tableName =  args(0)
//  print(tableName)

  val sourceFunction = SqlServerSource.builder[String]
    .hostname(SqlServer_ADDRESS)
    .port(SqlServer_PORT)
    .database(DATABASE)// monitor sqlserver database
    .tableList(TABLE_LIST) // monitor products table
    .username(USER_NAME)
    .password(USER_PASSWORD)
    .startupOptions(StartupOptions.initial())
    .deserializer(new JsonDebeziumDeserializationSchema())
    .build()


  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.addSource(sourceFunction).print.setParallelism(1) // use parallelism 1 for sink to keep message ordering

  env.execute()


}
