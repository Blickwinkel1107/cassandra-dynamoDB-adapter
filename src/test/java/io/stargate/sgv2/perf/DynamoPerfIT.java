package io.stargate.sgv2.perf;

import static org.junit.jupiter.api.Assertions.*;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import java.util.*;
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
  public void testDeleteTable() {
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
    for (CreateTableRequest req : requests) {
      proxyClient.createTable(req);
    }

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    int j = 0;
    for (CreateTableRequest req : requests) {
      proxyClient.deleteTable(req.getTableName());
      System.out.println(j);
      ++j;
    }
    stopWatch.stop();
    System.out.println("Latency: " + stopWatch.getTime() * 1.0 / tableNum + "ms");
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

  @Test
  public void testPutItem() {
    final CreateTableRequest createTableRequest =
        new CreateTableRequest()
            .withTableName("testTable")
            .withProvisionedThroughput(
                new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L))
            .withKeySchema(new KeySchemaElement("Name", KeyType.HASH))
            .withAttributeDefinitions(new AttributeDefinition("Name", ScalarAttributeType.S));
    proxyClient.createTable(createTableRequest);

    // test data
    Map<String, Object> dict = new HashMap<>();
    dict.put("integerList", Arrays.asList(0, 1, 2));
    dict.put("stringList", Arrays.asList("aa", "bb"));
    dict.put("hashMap", new HashMap<>());
    dict.put("doubleSet", new HashSet<>(Arrays.asList(1.0, 2.0)));
    final Item itemTemplate =
        new Item()
            .withPrimaryKey("Name", "testName")
            .withNumber("Serial", 123.0)
            .withString("ISBN", "121-1111111111")
            .withStringSet("Authors", new HashSet<String>(Arrays.asList("Author21", "Author 22")))
            .withNumber("Price", 20.1)
            .withString("Dimensions", "8.5x11.0x.75")
            .withNumber("PageCount", 500)
            .withBoolean("InPublication", true)
            .withString("ProductCategory", "Book")
            .withMap("Dict", dict);

    // DB initialize
    final DynamoDB proxyDynamoDB = new DynamoDB(proxyClient); // CDB
    final Table proxyTable = proxyDynamoDB.getTable(createTableRequest.getTableName());

    final int testNum = 100;
    ArrayList<String> keys = new ArrayList<>();
    for (int i = 0; i < testNum; ++i) {
      keys.add(getSaltString());
    }
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    int j = 0;
    for (String key : keys) {
      proxyTable.putItem(itemTemplate.withPrimaryKey("Name", key));
      System.out.println(j);
      ++j;
    }
    stopWatch.stop();
    System.out.println("Latency: " + stopWatch.getTime() * 1.0 / testNum + "ms");
    proxyClient.deleteTable(createTableRequest.getTableName());
  }

  @Test
  public void testDeleteItem() {
    final CreateTableRequest createTableRequest =
        new CreateTableRequest()
            .withTableName("testTable")
            .withProvisionedThroughput(
                new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L))
            .withKeySchema(new KeySchemaElement("Name", KeyType.HASH))
            .withAttributeDefinitions(new AttributeDefinition("Name", ScalarAttributeType.S));
    proxyClient.createTable(createTableRequest);

    // test data
    Map<String, Object> dict = new HashMap<>();
    dict.put("integerList", Arrays.asList(0, 1, 2));
    dict.put("stringList", Arrays.asList("aa", "bb"));
    dict.put("hashMap", new HashMap<>());
    dict.put("doubleSet", new HashSet<>(Arrays.asList(1.0, 2.0)));
    final Item itemTemplate =
        new Item()
            .withPrimaryKey("Name", "testName")
            .withNumber("Serial", 123.0)
            .withString("ISBN", "121-1111111111")
            .withStringSet("Authors", new HashSet<String>(Arrays.asList("Author21", "Author 22")))
            .withNumber("Price", 20.1)
            .withString("Dimensions", "8.5x11.0x.75")
            .withNumber("PageCount", 500)
            .withBoolean("InPublication", true)
            .withString("ProductCategory", "Book")
            .withMap("Dict", dict);

    // DB initialize
    final DynamoDB proxyDynamoDB = new DynamoDB(proxyClient); // CDB
    final Table proxyTable = proxyDynamoDB.getTable(createTableRequest.getTableName());
    DeleteItemSpec deleteItemSpec;

    final int testNum = 100;
    ArrayList<String> keys = new ArrayList<>();
    for (int i = 0; i < testNum; ++i) {
      keys.add(getSaltString());
    }
    for (String key : keys) {
      proxyTable.putItem(itemTemplate.withPrimaryKey("Name", key));
    }
    deleteItemSpec =
        new DeleteItemSpec()
            .withPrimaryKey("Name", "testName")
            .withConditionExpression("#S = :vSerial AND #I = :vIsbn")
            .withNameMap(new NameMap().with("#S", "Serial").with("#I", "ISBN"))
            .withValueMap(
                new ValueMap().withNumber(":vSerial", 123).withString(":vIsbn", "121-1111111111"));
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    int j = 0;
    for (String key : keys) {
      proxyTable.deleteItem(deleteItemSpec.withPrimaryKey("Name", key));
      System.out.println(j);
      ++j;
    }
    stopWatch.stop();
    System.out.println("Latency: " + stopWatch.getTime() * 1.0 / testNum + "ms");
    proxyClient.deleteTable(createTableRequest.getTableName());
  }

  @Test
  public void testGetItem() {
    final CreateTableRequest createTableRequest =
        new CreateTableRequest()
            .withTableName("testTable")
            .withProvisionedThroughput(
                new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L))
            .withKeySchema(new KeySchemaElement("Name", KeyType.HASH))
            .withAttributeDefinitions(new AttributeDefinition("Name", ScalarAttributeType.S));
    proxyClient.createTable(createTableRequest);
    // DB initialize
    final DynamoDB proxyDynamoDB = new DynamoDB(proxyClient); // CDB
    final Table proxyTable = proxyDynamoDB.getTable(createTableRequest.getTableName());

    // test data
    Map<String, Object> dict = new HashMap<>();
    dict.put("integerList", Arrays.asList(0, 1, 2));
    dict.put("stringList", Arrays.asList("aa", "bb"));
    Item item =
        new Item()
            .withPrimaryKey("Name", "simpleName")
            .withNumber("Serial", 23)
            .withNumber("Price", 10.0)
            .withList("Authors", Arrays.asList("Author21", "Author 22", dict, "Author44"))
            .withStringSet("StringSet", "ss1", "ss2", "ss3")
            .withNumberSet("NumberSet", 2, 4, 5)
            .withMap("dict", dict);

    String projection =
        "#N, Authors[0], Authors[2].#IL[0], Authors[3], #D.stringList[0], #D.#IL[1], NumberSet, StringSet[0]";
    Map<String, String> nameMap =
        new HashMap() {
          {
            put("#N", "Name");
            put("#D", "dict");
            put("#IL", "integerList");
          }
        };
    ArrayList<PrimaryKey> keys = new ArrayList<>();
    final int testNum = 100;
    for (int i = 0; i < testNum; ++i) {
      PrimaryKey key = new PrimaryKey("Name", getSaltString());
      keys.add(key);
      proxyTable.putItem(item.withPrimaryKey(key));
    }
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    int j = 0;
    for (PrimaryKey key : keys) {
      proxyTable.getItem(key, projection, nameMap);
      System.out.println(j);
      ++j;
    }
    stopWatch.stop();
    System.out.println("Latency: " + stopWatch.getTime() * 1.0 / testNum + "ms");
    proxyClient.deleteTable(createTableRequest.getTableName());
  }

  @Test
  public void testQuery() {
    CreateTableRequest createTableRequest =
        new CreateTableRequest()
            .withTableName("tableName")
            .withProvisionedThroughput(
                new ProvisionedThroughput()
                    .withReadCapacityUnits(100L)
                    .withWriteCapacityUnits(100L))
            .withKeySchema(
                new KeySchemaElement("Username", KeyType.HASH),
                new KeySchemaElement("Birthday", KeyType.RANGE))
            .withAttributeDefinitions(
                new AttributeDefinition("Username", ScalarAttributeType.S),
                new AttributeDefinition("Birthday", ScalarAttributeType.N));
    proxyClient.createTable(createTableRequest);
    // DB initialize
    final DynamoDB proxyDynamoDB = new DynamoDB(proxyClient); // CDB
    final Table proxyTable = proxyDynamoDB.getTable(createTableRequest.getTableName());

    // test data
    Set<Item> items = new HashSet<>();
    for (int i = 0; i < 40; ++i) {
      items.add(
          new Item()
              .withPrimaryKey("Username", "alice", "Birthday", 20000101 + i)
              .withString("Sex", "F")
              .withNumber("Deposit", 100)
              .withNumber("Loan", 200)
              .withList("history", 20220101, 20220201, 20220925));

      items.add(
          new Item()
              .withPrimaryKey("Username", "alice", "Birthday", 19801231 + i)
              .withString("Sex", "F")
              .withNumber("Deposit", 2000));
      items.add(
          new Item()
              .withPrimaryKey("Username", "alice", "Birthday", 20000304 + i)
              .withString("Sex", "F"));
      items.add(
          new Item()
              .withPrimaryKey("Username", "bob", "Birthday", 20000101 + i)
              .withString("Sex", "M"));

      Map<String, Object> dict = new HashMap<>();
      Map<String, Object> dict2 = new HashMap<>();
      dict.put("outer", dict2);
      List<Object> list = new ArrayList<>();
      Map<String, Object> dict3 = new HashMap<>();
      list.add(dict3);
      dict2.put("lst", list);
      Map<String, Object> dict4 = new HashMap<>();
      dict3.put("inner", dict4);
      dict4.put("nested", Arrays.asList(Arrays.asList(Arrays.asList(1, 2, 3))));

      items.add(
          new Item()
              .withPrimaryKey("Username", "Charlie", "Birthday", 20100801 + i)
              .withMap("dict", dict));
    }

    for (Item item : items) {
      proxyTable.putItem(item);
    }
    StopWatch stopWatch = new StopWatch();
    QuerySpec query =
        new QuerySpec()
            .withKeyConditionExpression("Username = :name AND Birthday >= :bday")
            .withValueMap(
                new ValueMap().withString(":name", "alice").withNumber(":bday", 19801231));
    final int testNum = 100;
    stopWatch.start();
    for (int j = 0; j < testNum; ++j) {
      proxyTable.query(query);
      System.out.println(j);
    }
    stopWatch.stop();
    System.out.println("Latency: " + stopWatch.getTime() * 1.0 / testNum + "ms");
    proxyClient.deleteTable(createTableRequest.getTableName());
  }
}
