import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.OFF);
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header",true).csv("src/main/resources/data/input.csv");

        dataset = dataset.select("LocId","Location").filter("LocId > 1000 AND Location like 'Af%'").distinct().cache();
        dataset.show();
        System.out.println(dataset.count());

        spark.close();
    }
}
