package io.stargate.sgv2.perf;

import static org.junit.jupiter.api.Assertions.*;

import com.amazonaws.services.dynamodbv2.model.*;
import java.util.ArrayList;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DynamoPerfIT extends io.stargate.sgv2.perf.DynamoBase {
  @Test
  public void testCreateTable() {
    final int tableNum = 10;
    ArrayList<CreateTableRequest> requests = new ArrayList<>();
    for (int i = 0; i < tableNum; ++i) {
      final String tableName = getSaltString();
      final CreateTableRequest req =
          new CreateTableRequest()
              .withTableName(tableName)
              .withProvisionedThroughput(
                  new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L))
              .withKeySchema(new KeySchemaElement("Name", KeyType.HASH))
              .withAttributeDefinitions(new AttributeDefinition("Name", ScalarAttributeType.S));
      requests.add(req);
    }
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    int j = 0;
    for (CreateTableRequest req : requests) {
      proxyClient.createTable(req);
      System.out.println(j);
      ++j;
    }
    stopWatch.stop();
    System.out.println("Latency: " + stopWatch.getTime() * 1.0 / tableNum + "ms");

    for (CreateTableRequest req : requests) {
      proxyClient.deleteTable(req.getTableName());
    }
  }

  @Test
  public void testListTables() {
    final int tableNum = 5;
    ArrayList<CreateTableRequest> createTableRequests = new ArrayList<>();
    for (int i = 0; i < tableNum; ++i) {
      final String tableName = getSaltString();
      final CreateTableRequest req =
          new CreateTableRequest()
              .withTableName(tableName)
              .withProvisionedThroughput(
                  new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L))
              .withKeySchema(new KeySchemaElement("Name", KeyType.HASH))
              .withAttributeDefinitions(new AttributeDefinition("Name", ScalarAttributeType.S));
      createTableRequests.add(req);
    }
    for (CreateTableRequest req : createTableRequests) {
      proxyClient.createTable(req);
    }

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    final int testNum = 100;
    for (int i = 0; i < testNum; ++i) {
      proxyClient.listTables();
      System.out.println(i);
    }
    stopWatch.stop();
    System.out.println("Latency: " + stopWatch.getTime() * 1.0 / testNum + "ms");

    for (CreateTableRequest req : createTableRequests) {
      proxyClient.deleteTable(req.getTableName());
    }
  }
}
