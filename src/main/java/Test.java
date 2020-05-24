import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("TestCategorization").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        context.textFile("src/main/resources/data/input.txt")
                .flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .filter(x -> x.length() > 1 && !x.equals("-->") && !x.matches("[0-9]{2}:[0-9]{2}.[0-9]{3}"))
                .map(v -> v.replace(".", "").replace("'", "").replaceAll("[0-9]{1,10}", ""))
                .mapToPair(v -> new Tuple2<>(v, 1L))
                .reduceByKey((e, v) -> e + v)
                .mapToPair(v -> new Tuple2<>(v._2, v._1))
                .filter(v -> v._1 > 10 && v._2.length() > 4)
                .sortByKey(true)
                .foreach(x -> System.out.println(x._2 + ": " + x._1));
        context.close();

    }
}
