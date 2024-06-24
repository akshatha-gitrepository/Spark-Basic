package de.rondiplomatico.spark.candy;

import com.google.common.collect.Iterables;
import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Candy;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.base.data.Deco;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Exercises for the basic spark section of the course.
 *
 * @since 2022-06-22
 * @author wirtzd
 *
 */
@SuppressWarnings("java:S100")
public class SparkBasics extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkBasics.class);

    /**
     * Local field containing the cities map.
     * (Placed here for a specific demonstration around serializability and spark)
     */
    private Map<String, String> cities = Utils.getHomeCities();
    
    FunctionalJava fn=new FunctionalJava();
    		

    /**
     * Configure your environment to run this class for section 2.
     *
     * @param args
     */
    public static void main(String[] args) {
        // Create a new instance of the SparkBasics exercise set.
        SparkBasics s = new SparkBasics();

        /**
         * E1: Generate crushes as RDD
         */
         JavaRDD<Crush> rdd = s.e1_crushRDD(1234);
         
         long size= rdd.count();
         long numOfPartitions = rdd.getNumPartitions();
        /*
         * TODO E1: Log the number of partitions and elements in the created RDD.
         */
         log.info("the number of partitions {} and elements in the created RDD {}",numOfPartitions,size);
        /**
         * E2: Filtering
         */
         s.e2_countCandiesRDD(rdd);

        /**
         * E3: Grouping
         */
         s.e3_countByColorRDD(rdd);

        /**
         * E4: Lookup
         */
         s.e4_cityLookupRDD(rdd);
    }

    /**
     * Creates a RDD of n crushes
     *
     * @param n
     * @return the rdd
     */
    public JavaRDD<Crush> e1_crushRDD(int n) {
        /*
         * TODO E1: Create crush RDD
         *
         * Use the functions from FunctionalJava to create some crushes and parallelize them using the java spark context
         */
    	FunctionalJava fn = new FunctionalJava();
    	
    	SparkBasics sb = new SparkBasics();
    	JavaRDD<Crush> data = sb.getJavaSparkContext().parallelize(fn.e1_crush(n));
        return data;
    }

    /**
     * Implements the various counting questions from {@link FunctionalJava} using spark
     *
     * @param crushes
     */
    public void e2_countCandiesRDD(JavaRDD<Crush> crushes) {
        /*
         * TODO E2: Filtering
         *
         * Implement "How many red striped candies have been crushed?"
         */
    	Long result = crushes.map(Crush::getCandy)
                .filter(c -> c.getColor().equals(Color.RED) &&
                             (c.getDeco().equals(Deco.VSTRIPES) || c.getDeco().equals(Deco.HSTRIPES)))
                .count();

        log.info("The Total Number of Red Striped Candies Crushed are {}", result);
        /*
         * TODO E2: Filtering
         *
         * Count how many wrapped candies have been crushed between 12-13 o'clock and log the results
         */
        LocalTime start = LocalTime.of(12, 0);
        LocalTime end = LocalTime.of(13, 0);

        
       Long wrappedCandies = crushes.filter(c->c.getCandy().getDeco().equals(Deco.WRAPPED))
    		   .filter(c->c.getTime().isAfter(start) && c.getTime().isBefore(end)).count();
       log.info("The crush data contains {} wrapped candies crushed between 12-13 o'clock!", wrappedCandies);

    }

    /**
     * Performs various counts using spark
     *
     * @param crushes
     */
    public void e3_countByColorRDD(JavaRDD<Crush> crushes) {
        /*
         * TODO E3: Grouping
         * Implement FunctionalJava-E3 using Spark!
         * - How many Candies are crushed per color?
         * - Stick with the functional flow "group, count, collect"
         * - Log your results.
         *
         * Hint: Iterables::size is convenient should you need to count the number of elements of an iterator.
         */
    	JavaPairRDD<Color, Integer> colorPairs  = crushes.mapToPair(c->new Tuple2<>(c.getCandy().getColor(),1));
    	JavaPairRDD<Color, Integer> colorcounts = colorPairs.reduceByKey(Integer::sum);
    	List<Tuple2<Color,Integer>> res = colorcounts.collect();
    	res.forEach(t->log.info("The crush data contains {} {} candies",t._2,t._1));
    			

        /*
         * TODO E3: (Bonus question)
         *
         * Implement "How many blue candies have been crushed per decoration type?"
         * - Avoid the groupBy() transformation - explore what better functions are available on JavaPairRDD!
         * - Can you also simplify the implementation of the first question similarly?
         */
//        Map<Deco,Long> blueCandies = crushes.map(c->c.getCandy())
//        		.filter(c->c.getColor().equals(Color.BLUE))
//        		.keyBy(c->c.getDeco())
//        		.countByKey();
//        blueCandies.forEach((c, i) -> log.info("The crush data contains {} {} blue candies", i, c));
    	 JavaRDD<Candy> blueCandies = crushes
                 .map(Crush::getCandy)
                 .filter(candy -> candy.getColor().equals(Color.BLUE));
    	 JavaPairRDD<Deco, Integer> decoPairs = blueCandies
                 .mapToPair(candy -> new Tuple2<>(candy.getDeco(), 1));
    	 JavaPairRDD<Deco, Integer> decoCounts = decoPairs
                 .reduceByKey(Integer::sum);
    	 Map<Deco, Integer> result = decoCounts.collectAsMap();
         result.forEach((c, i) -> log.info("The crush data contains {} {} blue candies", i, c));

        /*
         * Fast variant: Let spark do the counting!
         */
        Map<Color,Long> blueCandies2 = crushes.keyBy(c->c.getCandy().getColor())
        		.countByKey();
        blueCandies2.forEach((c, i) -> log.info("The crush data contains {} {} candies", i, c));
        
    }

    /**
     * Performs some statistics on crushes using spark and lookups.
     *
     * @param crushes
     */
    public void e4_cityLookupRDD(JavaRDD<Crush> crushes) {

        /*
         * TODO E4: Lookups
         *
         * - Understand this implementation of counting cities using a count map
         * - Run the method
         * - Investigate what might be going wrong here?
         */
//        Map<String, Integer> counts = new HashMap<>();
//        crushes.foreach(c -> {
//            String city = Utils.getHomeCities().get(c.getUser());
//            int newcnt = counts.getOrDefault(city, 0) + 1;
//            counts.put(city, newcnt);
//            log.info("Counting crush in {}, totalling {}", city, newcnt);
//        });
//        log.info("Counted crushes in {} cities", counts.size());
//        counts.forEach((c, i) -> log.info("There are {} crushes in {}", i, c));

        /*
         * TODO E4: Lookups
         *
         * Implement the counting as result of the transformation, using
         * - the class field "cities" as lookup (similar to FJ-E4)
         * - countByValue() as action
         * - Log your results
         * - Run the code
         */
       final Map<String, String> city = cities;
       
       Map<String, Long> countCity = crushes.map(c->city.get(c.getUser())).countByValue();
       countCity.forEach((c,i)-> log.info("There are {} crushes in {}", i, c));
        

        /*
         * TODO E4: Lookups
         * How many candies in Ismaning between 14-15 o'clock, counted by color?
         * Use all you've learned before.
         */
       LocalTime start1 = LocalTime.of(14, 0);
       LocalTime end1 = LocalTime.of(15, 0);
       
       Map<Color,Long> countColor = crushes.filter(c->"Ismaning".equals(city.get(c.getUser())))
    		   .filter(c->c.getTime().isAfter(start1) && c.getTime().isBefore(end1))
    		   .map(c->c.getCandy().getColor())
    		   .countByValue();
       countColor.forEach((c,i)-> log.info("There are {} color  {} candies", c, i));
    				   
        
    }

}
