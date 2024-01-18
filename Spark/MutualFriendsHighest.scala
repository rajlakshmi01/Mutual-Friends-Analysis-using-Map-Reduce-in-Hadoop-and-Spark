//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext

import scala.collection.mutable

val start = System.currentTimeMillis()
val inputFile = sc.textFile("Downloads/CS6304_Spark_Demo-master/Mutualfriends.txt")
val outputDir = ("Downloads/output")

// Prompt the user for the number of lines to read from the file.
println("Enter the number of lines to read from the file:")
val numLinesToProcess = Integer.parseInt(readLine())

// Read the first n lines from the file.
val friendPairs = inputFile.take(numLinesToProcess).flatMap(line => {
  val parts = line.split("\t")
  if (parts.length == 2) {
  val user = parts(0)
  val friends = parts(1).split(",").toList
  
  friends.map(friend => {
          if (user.toInt < friend.toInt) (s"$user,$friend", friends) else (s"$friend,$user", friends)
        })
      } else Nil
    })

val start= System.currentTimeMillis()

// Convert the array of (String, List[String]) tuples to an RDD[K, V] object.
val friendPairsRDD = sc.parallelize(friendPairs)

// Iterate over the lines and update the mutual friends count for each pair of friends.
val mutualFriends = friendPairsRDD.groupByKey().mapValues(iter => {
  val friendsLists = iter.toList
  if (friendsLists.size > 1) friendsLists.reduce((list1, list2) => list1.intersect(list2)) else Nil
}).filter(_._2.nonEmpty)

    // Find the pairs with the maximum number of mutual friends
    val maxCount = mutualFriends.map(_._2.size).max()

    val pairsWithMaxMutualFriends = mutualFriends.filter(_._2.size == maxCount)

    pairsWithMaxMutualFriends.saveAsTextFile(outputDir + "/maxMutualFriends")

    // Filter pairs with mutual friends starting with '1' or '5'
val filteredPairs = mutualFriends.filter {
  case (_, values) =>
    values.exists(friend => friend.startsWith("1") || friend.startsWith("5"))
}

    filteredPairs.saveAsTextFile(outputDir + "/filteredPairs")

    // Format the pairs with max mutual friends
val formattedPairsWithMaxMutualFriends = pairsWithMaxMutualFriends.map {
  case (pair, mutualFriendsList) => 
    s"Friends with Highest Mutual Friends Count:\t$pair\nMutual Friends Count:\t${mutualFriendsList.size}"
}

// Format the filtered pairs
val formattedFilteredPairs = filteredPairs.map {
  case (pair, mutualFriendsList) => s"$pair\t${mutualFriendsList.mkString(",")}"
}

// Merge the two formatted RDDs
val mergedResults = formattedPairsWithMaxMutualFriends ++ formattedFilteredPairs

// Coalesce the results into a single partition to get a single output file
val coalescedResults = mergedResults.coalesce(1)

// Save the results to the desired output directory
coalescedResults.saveAsTextFile(outputDir + "/finalResults")
val end = System.currentTimeMillis()
val executionTimeInMs = end - start

// Print the execution time in milliseconds to the console.
println(s"The execution time in milliseconds is $executionTimeInMs")

sc.stop()
