package com.ashvina.iceberg;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

public class SampleTableGenerationUtil {
  private final String icebergNs = "iceberg_ns";
  private final String tableName = "sales";
  private final String tableFQN = icebergNs + "." + tableName;

  private static Path workingDir =
      Paths.get("/tmp", SampleTableGenerationUtil.class.getSimpleName() + "-working-dir");

  private SparkSession sparkSession;

  public static void main(String[] args) {
    SampleTableGenerationUtil util = new SampleTableGenerationUtil();
    util.generateIcebergSample();
  }

  public void generateIcebergSample() {
    createSparkSession();
    createSampleTable();
    createSampleCommits();
  }

  private void validateTable(int expectedCardinality) {
    long count = sparkSession.sql("SELECT * FROM " + tableFQN).count();
    assert (expectedCardinality == count);
  }

  private void createSampleTable() {
    if (sparkSession == null) {
      return;
    }

    sparkSession.sql(
        " CREATE TABLE "
            + tableFQN
            + "( "
            + "GeographyKey int, "
            + "SalesTerritoryName string, "
            + "EmployeeKey int "
            + ") "
            + "USING ICEBERG "
            + "PARTITIONED BY (GeographyKey) ");
  }

  private void createSampleCommits() {
    if (sparkSession == null) {
      return;
    }

    sparkSession.sql(
        "INSERT INTO "
            + tableFQN
            + " VALUES (1, 'CA', 1001), (2, 'NY', 1002), "
            + "(3, 'TX', 1003), (4, 'FL', 1004)");
    validateTable(4);

    sparkSession.sql(
        "INSERT INTO "
            + tableFQN
            + " VALUES (1, 'CA', 1006), (2, 'NY', 1007), "
            + "(1, 'CA', 1008), (2, 'NY', 1009), (3, 'TX', 1010), (4, 'FL', 1011)");
    validateTable(10);

    sparkSession.sql("UPDATE " + tableFQN + " SET EmployeeKey = 1111 WHERE EmployeeKey = 1011");
    validateTable(10);

    sparkSession.sql("DELETE FROM " + tableFQN + " WHERE EmployeeKey > 1008");
    validateTable(7);

    sparkSession.sql("INSERT INTO " + tableFQN + " VALUES (3, 'TX', 1010), (4, 'FL', 1011)");
    validateTable(9);
  }

  void createSparkSession() {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");
    sparkSession =
        SparkSession.builder()
            .appName(SampleTableGenerationUtil.class.getSimpleName())
            .master("local[4]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", "file://" + workingDir)
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate();
  }
}
