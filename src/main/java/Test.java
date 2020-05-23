import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        List<Double> data = new ArrayList<>();
        data.add(35.2);
        data.add(0.1);
        data.add(56.5);
        data.add(5.0);
        data.add(10.2);

        Logger.getLogger("org.apache").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("TestCategorization").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<Double> rdd = context.parallelize(data);

        JavaRDD<Tuple2<Double, Double>> rdd1 = rdd.map((x) -> new Tuple2<>(x, Math.sqrt(x)));

        rdd1.foreach(doubleDoubleTuple2 -> System.out.println(doubleDoubleTuple2._1 + " " + doubleDoubleTuple2));

        context.close();

    }
}
