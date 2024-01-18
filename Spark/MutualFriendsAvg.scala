import scala.collection.mutable
import scala.util.Try

val start1 = System.currentTimeMillis()
val inputFile = sc.textFile("hdfs://localhost:9000/user/rm3rn/InputFolder/soc-LiveJournal1Adj.txt")
val outputDir = ("hdfs://localhost:9000/user/rm3rn/OutputFolder05006/")


// Prompt the user for the number of lines to read from the file.
println("Enter the number of lines to read from the file:")
val numLinesToProcess = Integer.parseInt(readLine())

// Read the first `n` lines from the file.
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

  

// Convert the array of `(String, List[String])` tuples to an `RDD[K, V]` object.
val friendPairsRDD = sc.parallelize(friendPairs)

// Iterate over the lines and update the mutual friends count for each pair of friends.
val mutualFriends = friendPairsRDD.groupByKey().mapValues(iter => {
  val friendsLists = iter.toList
  if (friendsLists.size > 1) friendsLists.reduce((list1, list2) => list1.intersect(list2)) else Nil
})
val start2= System.currentTimeMillis()
// Find the total number of mutual friends
val totalMutualFriends = mutualFriends.map(_._2.size).sum
// Calculate the average mutual friends
val averageMutualFriends = totalMutualFriends.toDouble / mutualFriends.count()

// Convert the mutualFriends RDD to an RDD[(String, Int)] object.
val pairCountRDD = mutualFriends.map(pair => (pair._1, pair._2.size)).filter(_._2 > averageMutualFriends)

// Save the pairCountRDD RDD to the output directory.
pairCountRDD.saveAsTextFile(outputDir+"/pairCount.txt")



// Print the average mutual friends to the console
val result = s"The average number of mutual friends is $averageMutualFriends"
//println(result)

// Convert the result RDD to type RDD[String].
val resultFile = sc.parallelize(Seq(result)).map(_.toString)
resultFile.saveAsTextFile(outputDir+"/AvgMutualFriends.txt")

val end = System.currentTimeMillis()
val executionTimeInMs = end - (start2)

// Print the execution time in milliseconds to the console.
println(s"The execution time in milliseconds is $executionTimeInMs")
sc.stop()

