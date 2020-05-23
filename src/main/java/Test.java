import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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

        rdd.collect().forEach(System.out::println);

        System.out.println("--------------------");

        JavaRDD<Double> rdd1 = rdd.map((x)->Math.sqrt(x));

        rdd1.collect().forEach(System.out::println);
        context.close();

    }
}
