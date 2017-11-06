
import TutorialHelper._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object Tutorial {
  def main(args: Array[String]) {
    // Configure Twitter credentials using twitter.txt
    TutorialHelper.configureTwitterCredentials()

    val conf = new SparkConf().setMaster("local[*]").setAppName("twitter-test2")
    val sparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sparkContext, Seconds(5))

    val stream: DStream[Status] = TwitterUtils.createStream(ssc, None)

    // Each tweet comes as a twitter4j.Status object, which we can use to
    // extract hash tags. We use flatMap() since each status could have
    // ZERO OR MORE hashtags.
    val hashTags = stream.flatMap(status => status.getHashtagEntities)
    // Convert hashtag to (hashtag, 1) pair for future reduction.

    val hashTagPairs = hashTags.map(hashtag => ("#" + hashtag.getText, 1))
    // Use reduceByKeyAndWindow to reduce our hashtag pairs by summing their
    // counts over the last 10 seconds of batch intervals (in this case, 2 RDDs).

    // Reduce last 30 seconds of data, every 10 seconds
    val topCounts10 = hashTagPairs.reduceByKeyAndWindow((l, r) => {l + r}, Seconds(10))
    // topCounts10 will provide a new RDD for every window. Calling transform()
    // on each of these RDDs gives us a per-window transformation. We use
    // this transformation to sort each RDD by the hashtag counts. The FALSE
    // flag tells the sortBy() function to sort in descending order.

    val sortedTopCounts10 = topCounts10.transform(rdd =>
      rdd.sortBy(hashtagPair => hashtagPair._2, false))

    // Print popular hashtags.
    sortedTopCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (tag, count) => println("%s (%d tweets)".format(tag, count))}
    })



    ssc.start()
    ssc.awaitTermination()
  }
}