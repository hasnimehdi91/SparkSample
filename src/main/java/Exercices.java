import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Exercices {
    public static void main(String[] args) {
        List<Tuple2<Integer, Integer>> views = new ArrayList<>();
        views.add(new Tuple2<>(14, 96));
        views.add(new Tuple2<>(14, 97));
        views.add(new Tuple2<>(13, 96));
        views.add(new Tuple2<>(13, 96));
        views.add(new Tuple2<>(13, 96));
        views.add(new Tuple2<>(14, 99));
        views.add(new Tuple2<>(13, 100));

        List<Tuple2<Integer, Integer>> course = new ArrayList<>();

        course.add(new Tuple2<>(96, 1));
        course.add(new Tuple2<>(97, 1));
        course.add(new Tuple2<>(98, 1));
        course.add(new Tuple2<>(99, 1));
        course.add(new Tuple2<>(100, 2));
        course.add(new Tuple2<>(101, 3));
        course.add(new Tuple2<>(101, 3));
        course.add(new Tuple2<>(102, 3));
        course.add(new Tuple2<>(103, 3));
        course.add(new Tuple2<>(104, 3));
        course.add(new Tuple2<>(105, 3));
        course.add(new Tuple2<>(106, 3));
        course.add(new Tuple2<>(107, 3));
        course.add(new Tuple2<>(108, 3));
        course.add(new Tuple2<>(109, 3));

        Logger.getLogger("org.apache").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("TestCategorization").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        context.parallelize(course)
                .mapToPair(v -> new Tuple2<>(v._2, 1L))
                .reduceByKey((v, e) -> e + v).foreach(e-> System.out.println(e));

    }
}
