import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Community Collapse - GraphX Analysis")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set(
     "fs.defaultFS",
     "hdfs://localhost:9000"
    )    
    sc.setLogLevel("WARN")

    // ============================================================
    // 1. Read Hive-generated graph edges from HDFS
    // ============================================================

    val edgeFile =
      "/community_collapse/graph/edges/000000_0"

    val rawEdges = sc.textFile(edgeFile)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { line =>
        val parts = line.split("\t")

        (
          parts(0).trim,
          parts(1).trim,
          parts(2).trim.toInt
        )
      }
      .cache()

    // ============================================================
    // 2. Create a collision-free ID for every subreddit
    // ============================================================

    val subredditNames: RDD[String] =
      rawEdges
        .flatMap {
          case (source, target, _) =>
            Seq(source, target)
        }
        .distinct()
        .cache()

    val vertices: RDD[(Long, String)] =
      subredditNames
        .zipWithUniqueId()
        .map {
          case (name, id) =>
            (id, name)
        }
        .cache()

    // Reverse lookup:
    // subreddit name -> unique GraphX vertex ID

    val vertexIds: RDD[(String, Long)] =
      vertices.map {
        case (id, name) =>
          (name, id)
      }

    // ============================================================
    // 3. Convert subreddit names into GraphX edges
    // ============================================================

val sourceWithIds = rawEdges
  .map { case (source, target, weight) =>
    (source, (target, weight))
  }
  .join(vertexIds)
  .map { case (source, ((target, weight), sourceId)) =>
    (target, (sourceId, weight))
  }

val edges: RDD[Edge[Int]] =
  sourceWithIds
    .join(vertexIds)
    .map { case (target, ((sourceId, weight), targetId)) =>
      Edge(sourceId, targetId, weight)
    }
    .cache()
    // ============================================================
    // 4. Create GraphX graph
    // ============================================================

    val graph =
      Graph(vertices, edges)

    // Materialize graph
    val vertexCount = graph.vertices.count()
    val edgeCount = graph.edges.count()

    println()
    println("============================================================")
    println("        COMMUNITY COLLAPSE - GRAPHX ANALYSIS")
    println("============================================================")
    println()

    println(s"Number of communities (vertices): $vertexCount")
    println(s"Number of subreddit connections (edges): $edgeCount")

    // ============================================================
    // 5. Degree analysis
    // ============================================================

    val inDegrees =
      graph.inDegrees

    val outDegrees =
      graph.outDegrees

    val avgInDegree =
      inDegrees.map(_._2.toDouble).mean()

    val avgOutDegree =
      outDegrees.map(_._2.toDouble).mean()

    println()
    println("------------------------------------------------------------")
    println("DEGREE ANALYSIS")
    println("------------------------------------------------------------")

    println(f"Average in-degree : $avgInDegree%.2f")
    println(f"Average out-degree: $avgOutDegree%.2f")

    // ============================================================
    // 6. Top communities by out-degree
    // ============================================================

    val topOutDegree =
      outDegrees
        .join(vertices)
        .map {
          case (_, (degree, subreddit)) =>
            (degree, subreddit)
        }
        .sortByKey(ascending = false)
        .take(10)

    println()
    println("Top 10 communities by OUT-DEGREE:")
    println()

    topOutDegree.foreach {
      case (degree, subreddit) =>
        println(f"$subreddit%-30s $degree")
    }

    // ============================================================
    // 7. Top communities by in-degree
    // ============================================================

    val topInDegree =
      inDegrees
        .join(vertices)
        .map {
          case (_, (degree, subreddit)) =>
            (degree, subreddit)
        }
        .sortByKey(ascending = false)
        .take(10)

    println()
    println("Top 10 communities by IN-DEGREE:")
    println()

    topInDegree.foreach {
      case (degree, subreddit) =>
        println(f"$subreddit%-30s $degree")
    }

    // ============================================================
    // 8. Connected Components
    // ============================================================

    val components =
      graph.connectedComponents()

    val numberOfComponents =
      components.vertices
        .map {
          case (_, componentId) =>
            componentId
        }
        .distinct()
        .count()

    println()
    println("------------------------------------------------------------")
    println("CONNECTIVITY")
    println("------------------------------------------------------------")

    println(s"Number of connected components: $numberOfComponents")

    // ============================================================
    // 9. PageRank
    // ============================================================

    val pageRank =
      graph.pageRank(0.0001).vertices

    val topPageRank =
      pageRank
        .join(vertices)
        .map {
          case (_, (rank, subreddit)) =>
            (rank, subreddit)
        }
        .sortByKey(ascending = false)
        .take(10)

    println()
    println("------------------------------------------------------------")
    println("TOP COMMUNITIES BY PAGERANK")
    println("------------------------------------------------------------")
    println()

    topPageRank.foreach {
      case (rank, subreddit) =>
        println(f"$subreddit%-30s $rank%.6f")
    }

    // ============================================================
    // 10. Save degree results
    // ============================================================

    val degreeOutput =
      graph.degrees
        .join(vertices)
        .map {
          case (_, (degree, subreddit)) =>
            s"$subreddit\t$degree"
        }

    degreeOutput.saveAsTextFile(
      "/community_collapse/graph/results/degree"
    )

    // ============================================================
    // 11. Save PageRank results
    // ============================================================

    val pageRankOutput =
      pageRank
        .join(vertices)
        .map {
          case (_, (rank, subreddit)) =>
            s"$subreddit\t$rank"
        }

    pageRankOutput.saveAsTextFile(
      "/community_collapse/graph/results/pagerank"
    )

    // ============================================================
    // 12. Finish
    // ============================================================

    println()
    println("============================================================")
    println("GraphX analysis completed successfully.")
    println()
    println("Results saved to:")
    println("  /community_collapse/graph/results/degree")
    println("  /community_collapse/graph/results/pagerank")
    println("============================================================")

    spark.stop()
  }
}
