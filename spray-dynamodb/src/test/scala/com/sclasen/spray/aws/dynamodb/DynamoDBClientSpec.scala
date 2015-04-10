package com.sclasen.spray.aws.dynamodb

import org.scalatest.WordSpec
import org.scalatest.Matchers
import akka.actor.ActorSystem
import akka.util.Timeout
import concurrent.Await
import concurrent.duration._
import com.amazonaws.services.dynamodbv2.model._

class DynamoDBClientSpec extends WordSpec with Matchers {

  "A DynamoDBClient" must {
    "List tables" in {
      val system = ActorSystem("test")
      val props = DynamoDBClientProps(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"), Timeout(100 seconds), system, system, endpoint = "http://localhost:8000")
      val client = new DynamoDBClient(props)
      try {

        import scala.collection.JavaConverters._
        val schema = List(new KeySchemaElement("a-name", KeyType.HASH))
        val attributes = List(new AttributeDefinition("a-name", ScalarAttributeType.S))
        val provisionedThroughput = new ProvisionedThroughput(10, 10)
        Await.result(client.sendCreateTable(new CreateTableRequest("a-table", schema.asJava).withAttributeDefinitions(attributes.asJava).withProvisionedThroughput(provisionedThroughput)), 100 seconds)

        val result = Await.result(client.sendListTables(new ListTablesRequest()), 100 seconds)
        println(result)
        result.getTableNames.size() should be >= 1
      } catch {
        case e: Exception =>
          println(e)
          e.printStackTrace()
      }
    }
  }

}
