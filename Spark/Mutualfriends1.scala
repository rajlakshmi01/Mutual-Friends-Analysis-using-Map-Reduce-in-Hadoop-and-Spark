import scala.collection.mutable
import scala.util.Try

val start = System.currentTimeMillis()
val inputFile = sc.textFile("Downloads/CS6304_Spark_Demo-master/Mutualfriends.txt")
val outputDir = ("Downloads/output02")

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
// Convert the array of (String, List[String]) tuples to an RDD[K, V] object.
val friendPairsRDD = sc.parallelize(friendPairs)

// Iterate over the lines and update the mutual friends count for each pair of friends.
val mutualFriends = friendPairsRDD.groupByKey().mapValues(iter => {
val friendsLists = iter.toList
if (friendsLists.size > 1) friendsLists.reduce((list1, list2) => list1.intersect(list2)) else Nil
}).filter(_._2.nonEmpty)
// Write the mutual friends to the output directory
mutualFriends.map(friendPair => friendPair._1 + "\t" + friendPair._2.mkString(",")).saveAsTextFile(outputDir)


val end = System.currentTimeMillis()
val executionTimeInMs = end - start

// Print the execution time in milliseconds to the console.
println(s"The execution time in milliseconds is $executionTimeInMs")


sc.stop()
