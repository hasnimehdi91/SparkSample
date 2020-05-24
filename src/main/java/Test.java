import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        List<String> data = new ArrayList<>();
        data.add("WARN: hello");
        data.add("ERROR: hello");
        data.add("ERROR: hello");
        data.add("INFO: hello");
        data.add("WARN: hello");

        Logger.getLogger("org.apache").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("TestCategorization").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        context.parallelize(data)
                .mapToPair(v -> new Tuple2<>(v.split(":")[0], 1L))
                .reduceByKey((e, v) -> e + v)
                .foreach(stringLongTuple2 -> System.out.println(stringLongTuple2._1 + ":" + stringLongTuple2._2));

        context.close();

    }
}
