/**
 * 
 */
package com.livefortech.spark.rdd;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author ravikumarvish
 *
 */
public class EmployeeAction {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Spark Action App").setMaster("local[1]");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> texts = context.textFile("in/numbers.txt");
		
		JavaRDD<String> textNumbers = texts.flatMap(text -> Arrays.asList(text.split("\t")).iterator());
		
		List<String> elements = textNumbers.collect();
		
		System.out.println("**********Elements********"+elements);
		
		System.out.println("Count : "+textNumbers.count());
		
		Map<String,Long> numberOccuranceCount = textNumbers.countByValue();
		
		Set<String> keys = numberOccuranceCount.keySet();
		
		for (String key : keys) {
			System.out.println(key + ": " +numberOccuranceCount.get(key));
		}
		
		List<String> takeElement = textNumbers.take(4);
		
		System.out.println("takeElement "+ takeElement);
		
		
		JavaRDD<Integer> numbers = textNumbers.map(text->Integer.valueOf(text));
		
		Integer total2 = numbers.reduce((x,y)->x+y);
		
		System.out.println("Total : "+total2);
		
		
		
		
		
		
		

	}

}
