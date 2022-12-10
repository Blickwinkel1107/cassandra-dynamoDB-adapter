package io.stargate.sgv2.perf;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import java.util.Properties;
import java.util.Random;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.*;

@TestClassOrder(ClassOrderer.DisplayName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DynamoBase {
  protected AmazonDynamoDB proxyClient;
  private static final String AUTH_TOKEN = "d74be0b7-e142-4d17-a2b6-aaee14c77353";

  @BeforeEach
  public void setup() {
    Properties props = System.getProperties();
    props.setProperty("aws.accessKeyId", AUTH_TOKEN);
    props.setProperty("aws.secretKey", "any-string");
    //    createKeyspace();
    AwsClientBuilder.EndpointConfiguration endpointConfiguration =
        new AwsClientBuilder.EndpointConfiguration("http://localhost:8082/v2", "any-string");
    proxyClient =
        AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(endpointConfiguration)
            .build();
  }

  protected void createKeyspace() {
    givenWithAuth()
        .contentType(ContentType.JSON)
        .when()
        .post(endpointPathForAllKeyspaces())
        .then()
        .statusCode(HttpStatus.SC_CREATED);
  }

  protected RequestSpecification givenWithAuth() {
    return givenWithAuthToken(AUTH_TOKEN);
  }

  protected RequestSpecification givenWithoutAuth() {
    return given();
  }

  protected RequestSpecification givenWithAuthToken(String authTokenValue) {
    return given().header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, authTokenValue);
  }

  protected String endpointPathForAllKeyspaces() {
    return "/v2/keyspace/create";
  }

  protected void assertException(AmazonServiceException expected, AmazonServiceException actual) {
    assertEquals(expected.getErrorCode(), actual.getErrorCode());
    assertEquals(expected.getErrorType(), actual.getErrorType());
    // Our system also records a unique identifier of the exception, which amazon does not
    assertTrue(expected.getMessage().contains(actual.getMessage()));
  }

  protected String getSaltString() {
    String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    StringBuilder salt = new StringBuilder();
    Random rnd = new Random();
    while (salt.length() < 18) { // length of the random string.
      int index = (int) (rnd.nextFloat() * SALTCHARS.length());
      salt.append(SALTCHARS.charAt(index));
    }
    return salt.toString();
  }
}
