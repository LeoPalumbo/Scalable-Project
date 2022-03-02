import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.commons.io.FileUtils

import java.io._
import scala.math.min

class FastaReader(val filename: String) extends Iterator[(String, String, String)] {
  private lazy val reader = new BufferedReader(new FileReader(filename))

  class FastaReadException(string: String) extends Exception(string)
  def hasNext() = reader.ready
  def next(): (String, String, String) = {
    // Read the tag line
    val tag = reader.readLine
    if (tag(0) != '>')
      throw new FastaReadException("record start expected")
    var sequencelist = ""
    // Read the sequence body
    do {
      reader.mark(512) // 512 is sufficient for a single tag line
      val line = reader.readLine
      if (line(0) != '>') sequencelist += line
      if (!reader.ready || line(0) == '>') {
        // Reached the end of the sequence
        if (reader.ready) reader.reset
        // Remove prepending '>'
        val tag2 = tag.drop(1).trim
        val id = tag2.split(Array(' ', '\t'))(0)
        return (id, tag2, sequencelist)
      }
    } while (reader.ready)
    // should never reach this...
    throw new FastaReadException("Error in file " + filename + " (tag=" + tag + ")")
  }
}

class NucleotideSequence(val filename: String){
  def read(position: Int): Option[Array[Char]]={
    val parsed = new FastaReader(filename);
    val sequences = parsed.map((x)=>x._3)
    return sequences.drop(position).take(1).toList.headOption match{
      case None => None
      case Some(seq) => Some(seq.toArray)
    }
  }
}

trait sequenceDistance[A]{
  def penaltyLeft: Int
  def penaltyUp: Int
  def select3(a: Int, b: Int, c: Int): Int
  def penaltyBuilder(size:Int): Array[Int]
  def Compute [B >: A, C>: A](s1: Array[B], s2: Array[C]): (Int, Option[Double], Int, Int) ={
    val firstLineDistances = penaltyBuilder(s2.size)
    val firstColumnDistances = penaltyBuilder(s1.size)
    val seqAndDist = s1.zip(firstColumnDistances)

    def partialDist[B >: A](left: (Int, Int, Int), up:(Int, Int, Int), diag: (Int, Int, Int), c1: B, c2: B): (Int, Int, Int) = {
      val selected = select3(left._1+penaltyLeft, up._1+penaltyUp, diag._1+Compare(c1, c2))	//si pensi min
      if (selected == diag._1+Compare(c1, c2) && c1 == c2){
        val matched=diag._2+1;
        val different=diag._3;
        return (selected, matched, different)
      }
      if (selected == diag._1+Compare(c1, c2) && c1 != c2){
        val matched=diag._2;
        val different=diag._3+1;
        return (selected, matched, different)
      }
      if (selected == up._1+penaltyUp){
        val matched = up._2;
        val different = up._3;
        return (selected, matched, different)
      }
      else //if (selected == left._1+penaltyLeft)
      {
        val matched = left._2;
        val different = left._3;
        return (selected, matched, different)
      }
    }

    def makeNewLine[B >:A, C>:A] (oldLine: Array[(Int, Int, Int)], charLine: Array[C], leftDistance: Int, leftChar: B, diag: Int): Array[(Int, Int, Int)] = {
      val charsAndDists = oldLine.zip(charLine).scanLeft(((leftDistance, 0, 0), (diag, 0, 0)))( (acc, item) => (partialDist(acc._1, item._1, acc._2, leftChar, item._2), item._1)) //il secondo elemento è la diagonale, è ottenuto ad ogni passo mettendo item._1, quindi l'elemento che attualmente si trova esattamente sopra
      charsAndDists.drop(1).map((x)=> x._1)
    }


    val result= seqAndDist.foldLeft((firstLineDistances, Array.fill(firstLineDistances.size)(0), Array.fill(firstLineDistances.size)(0)).zipped.toArray)((acc, item)=> (makeNewLine(acc, s2, item._2, item._1, if(acc(0)._1==0) 0 else item._2))) //l'if serve per il primissimo valore, quello in M[0,0]

    val p= if (result(s2.length-1)._2+result(s2.length-1)._3!=0) Some((result(s2.length-1)._3).toDouble/(result(s2.length-1)._2+result(s2.length-1)._3).toDouble) else None
    (result(s2.length-1)._1, p, (result(s2.length-1)._3), (result(s2.length-1)._2)); //1: score, 2: p-distance, 3: sostituzioni, 4: corretti
  }
  def Compare [B>: A, C>: A](m1: B, m2: C): Int

  def Score[B >: A, C>: A](s1: Array[B], s2: Array[C]): Int ={Compute(s1, s2)._1}
  def pDistance[B >: A, C>: A](s1: Array[B], s2: Array[C]): Option[Double]={Compute(s1, s2)._2}
  def substitutions[B >: A, C>: A](s1: Array[B], s2: Array[C]): Int={Compute(s1, s2)._3}
  def matching[B >: A, C>: A](s1: Array[B], s2: Array[C]): Int={Compute(s1, s2)._4}
}

class lDistance extends sequenceDistance[Char]{
  def penaltyLeft = 1;
  def penaltyUp = 1;
  def penaltyBuilder(size:Int): Array[Int] = (0 until size).toArray
  def select3(a: Int, b: Int, c: Int) = Math.min(Math.min(a, b), c);
  def Compare [B>: Char, C>: Char](c1: B, c2: C): Int = {if (c1==c2) 0 else 1}
}

class NeighbourJoining {

  var r_i = Map[Int, Double]()
  var distances = Map[(Int, Int), Double]()
  var d_i_j = Map[(Int, Int), Double]()
  var graph = Map[(Int, Int), Double]()
  var numOfSeq: Int = 0
  var numOfNodeTmp : Int = 0
  var last_node: Int = 0

  //var updateMatrix: Bool = false
  /*
   def NJ():
       if(terminationCond) // get only one value in the matrix
            break;

       // recursive
       if(firstUpdate)
            updateMatrix()

       setR_I()
       setD_I_J()
       joinSmallestNodes() //remove updateMatrix here

       firstUpdate = true

       NJ() // recursive call


   main:
        // compute the distance map
        init_NJ()
        NJ()
   */
  def NJ() : Unit = {
    var n : (Int, Int) = (0, 0)
    for (_ <- 0 until (numOfSeq - 2)){
      setR_I()
      setD_I_J()
      n = joinSmallestNodes() //remove updateMatrix here
      updateMatrix(n._1, n._2)
    }
    graph += ((last_node - 1, last_node - 2) -> distances.filterKeys(k=> k._1!=k._2).head._2)
  }





  def init(d: Map[(Int, Int), Double]): Unit = {
    // make matrix communative
    for ((k,v) <- d) {
      distances += ((k._1, k._2) -> v)
      distances += ((k._2, k._1) -> v)
    }

    numOfSeq = distances.max._1._1 + 1
    numOfNodeTmp = distances.max._1._1 + 1

    last_node = numOfSeq

    // init r_i can be half of that? |r_i| = num_seq
    for (i <- 0 until numOfSeq) {
      distances += ((i, i) -> 0)
    }
  }


  def setR_I(): Unit ={
    for ((k, v) <- distances) {
      if(r_i.contains(k._1)) {
        r_i += (k._1 -> (r_i.getOrElse(k._1, 0.0) + v))
      } else {
        r_i += (k._1 -> (0.0 + v))
      }
    }
//    for((k, v) <- r_i) {
//      r_i += (k -> v/(numOfNode - 2))
//    }

    r_i=r_i.mapValues(x=> x/(numOfSeq - 2))


  }


  def setD_I_J(): Unit ={
    d_i_j = Map[(Int, Int), Double]()
    var tmpMatrixDist: Map[(Int, Int), Double] = Map()
    tmpMatrixDist++=distances

    val tmpSuperiorMatrix = tmpMatrixDist.filterKeys(k=> k._1>k._2)

    /*var tmpSuperiorMatrix = Map[(Int, Int), Double]()
    for ((k, v) <- distances) {
      if(k._1 > k._2) {
        tmpSuperiorMatrix += ((k._1, k._2) -> v)
      }
    }
    for ((k,v) <- tmpSuperiorMatrix) {
      d_i_j += ((k._1, k._2) -> (v - r_i(k._1) - r_i(k._2)))
    }*/
    d_i_j = tmpSuperiorMatrix.transform((k, v) => v - r_i(k._1) - r_i(k._2))
  }

  def joinSmallestNodes(): (Int, Int) ={
    val min = d_i_j.minBy(_._2)

    val node_1 : Int = min._1._1
    val node_2 : Int = min._1._2

    val dist_node1: Double = 0.5 * distances(node_1, node_2) + 0.5 * (r_i(node_1) - r_i(node_2))
    val dist_node2: Double = 0.5 * distances(node_1, node_2) + 0.5 * (r_i(node_2) - r_i(node_1))
    graph += ((node_1, last_node) -> dist_node1)
    graph += ((node_2, last_node) -> dist_node2)
    last_node = last_node + 1
    (node_1, node_2)
  }

  def updateMatrix(node_1: Int, node_2: Int): Unit ={
    /*val i : Int = scala.math.max(node_1, node_2)
    val j : Int = scala.math.min(node_1, node_2)

    println(i)
    println(j)
    var tmpMatrix = distances.filter(x => (x._1 != i && x._2 != i))
    tmpMatrix = tmpMatrix.-((i, i))
    for ((k, v) <- tmpMatrix) {
      if (k._1 == j) {
        if (k._1 > k._2) {

          tmpMatrix = tmpMatrix.-((i, k._1))
          tmpMatrix = tmpMatrix.-((k._1, i))
          tmpMatrix = tmpMatrix.-((j, k._1))
          tmpMatrix = tmpMatrix.-((k._1, j))
          tmpMatrix = tmpMatrix.-((i, k._2))
          tmpMatrix = tmpMatrix.-((k._2, i))
          tmpMatrix = tmpMatrix.-((j, k._2))
          tmpMatrix = tmpMatrix.-((k._2, j))

          val newVal = (distances(i, k._2) + distances(j, k._2) - distances(i, j)) / 2

          tmpMatrix += ((numOfNodeTmp - 2, k._2) -> newVal)
          tmpMatrix += ((k._2, numOfNodeTmp - 2) -> newVal)
        }
      }
    }

    tmpMatrix += ((numOfNodeTmp - 2, numOfNodeTmp - 2) -> 0.0)

    distances = tmpMatrix*/

    var tmpMatrix = distances

    /*
    for ((k, v) <- tmpMatrix){
      if(k._1 == node_1 || k._2 == node_1 || k._1 == node_2 || k._2 == node_2)
      tmpMatrix = tmpMatrix.-((k))
    }
    */
    tmpMatrix = tmpMatrix.filterKeys(k => k._1 != node_1 && k._1 != node_2 && k._2 != node_1 && k._2 != node_2)
    println(distances)

    for ((k, v) <- tmpMatrix){
      val newVal = (distances(k._1, node_1) + distances(k._1, node_2) - distances(node_1, node_2)) / 2
      tmpMatrix = tmpMatrix.+((k._1, last_node) -> newVal)
      tmpMatrix = tmpMatrix.+((last_node, k._1) -> newVal)
    }

    tmpMatrix = tmpMatrix.+((last_node, last_node) -> 0.0)

    distances = tmpMatrix
  }
}


object main{
  def main(argv: Array[String]){
    val conf = new SparkConf().setAppName("Phylogenetic Tree").setMaster("local[*]")

    /*
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    */

    /*
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.dynamicAllocation.minExecutors","1")
    conf.set("spark.dynamicAllocation.maxExecutors","16")
    */

    val sc = new SparkContext(conf)

    /*val files = new java.io.File("dataset").listFiles.filter(_.getName.endsWith(".fasta")).map((x)=>"dataset/"+x.getName)

    val names = files.map((x)=> new FastaReader(x).next()._1)zip(0 until files.size)
    val sequences = files.map((x)=> new NucleotideSequence(x).read(0))

    println(files(0))
    println(names(0))
    println(sequences(0))

    var pairs : List[((Int, Option[Array[Char]]), (Int, Option[Array[Char]]))] = List[((Int, Option[Array[Char]]), (Int, Option[Array[Char]]))]()

    for (i <- 0 until sequences.size){
      for (j<- 0 until i){
        pairs = pairs :+ ((i, sequences(i)), (j, sequences(j)))
      }
    }


    // Calcolo parallelo delle distanze


    val ppairs = sc.parallelize(pairs)
    /*
        val distances = ppairs.map((x)=>((x._1._1, x._2._1), x._1._2 match {
          case None => 1.0
          case Some(s1) => x._2._2 match {
            case None => 1.0
            case Some(s2) =>
              {
                val ld = new lDistance;
                ld.pDistance (s1, s2) match{
                  case None => 1.0
                  case Some(d) => d
                }
              }
          }
        })).collect().toMap;
    */

    val distances = ppairs.map((x)=>((x._1._1, x._2._1), x._1._2 match {
      case None => 1.0
      case Some(s1) => x._2._2 match {
        case None => 1.0
        case Some(s2) =>
        {
          val ld = new lDistance;
          ld.substitutions (s1, s2)
        }
      }
    })).collect().toMap;

    */
    //println(distances)
    var dist : Map[(Int, Int), Double] = Map((3,2) -> 14, (3,1) -> 18, (3,0) -> 27, (2,1) -> 12, (2,0) -> 21, (1,0) -> 17)
    //println(distances.size)
    //distances = Map((7,1) -> 0.0340485544474665, (7,5) -> 0.04287392983045157, (7,6) -> 0.03546718613555451, (5,0) -> 0.004233870967741936, (5,2) -> 0.010563498738435661, (7,4) -> 0.039741779301997175, (5,1) -> 0.01056976041876384, (4,0) -> 0.01908281538719973, (6,4) -> 0.011216710884239514, (3,1) -> 0.0017828310010764262, (6,1) -> 0.003658086384535356, (4,1) -> 0.009476124869787292, (6,2) -> 0.0032298220233489216, (2,0) -> 0.011415678879310345, (3,0) -> 0.01184866029352363, (6,5) -> 0.012515098644477252, (6,3) -> 0.003732723543060833, (7,3) -> 0.034229746558513685, (5,4) -> 0.01844200342638315, (3,2) -> 0.0017157852240613646, (4,2) -> 0.009224346889307837, (5,3) -> 0.010827896966843768, (2,1) -> 0.0016490543178299792, (4,3) -> 0.009792374735000168, (6,0) -> 0.013470388659343613, (7,2) -> 0.0340775162474324, (1,0) -> 0.01159624886558435, (7,0) -> 0.043454863446791336)

    val neighbourJoining = new NeighbourJoining()
    neighbourJoining.init(dist)
    /*println("Matrix " + neighbourJoining.distances)
    println("Size: " + neighbourJoining.distances.size)
    println("Num of nodes " + neighbourJoining.numOfSeq)
    neighbourJoining.setR_I()
    println(neighbourJoining.r_i)
    neighbourJoining.setD_I_J()

    neighbourJoining.joinSmallestNodes()
    println("Nuova matrice da passare alla chiamata ricorsiva")
    println(neighbourJoining.distances)
    println(neighbourJoining.distances.size)
    println("Grafo fino ad ora")
    println(neighbourJoining.graph)

    neighbourJoining.setR_I()

    neighbourJoining.setD_I_J()
    neighbourJoining.joinSmallestNodes()
    println("Nuova matrice da passare alla chiamata ricorsiva")
    println(neighbourJoining.distances)
    println(neighbourJoining.distances.size)
    println(neighbourJoining.numOfSeq)
    println("Grafo fino ad ora")
    println(neighbourJoining.graph)


    println(neighbourJoining.distances)

    neighbourJoining.graph += ((neighbourJoining.last_node - 1, neighbourJoining.last_node - 2) -> neighbourJoining.distances(neighbourJoining.last_node - 1, neighbourJoining.last_node - 2))

    println(neighbourJoining.graph)*/
    /*while (neighbourJoining.distances.size > 4) {
      dist = neighbourJoining.distances
    }*/

    neighbourJoining.NJ()
    println(neighbourJoining.graph)
    return

    //println(min)
    //println(min.keys)
    //println(min.keys.head._1)
    //println(min.keys.head._2)
    //println(min.keys.toSeq)
    //neighbourJoining.calculateBranchLength(min.keys.take(1))
    val vertices = neighbourJoining.d_i_j.toSeq
    println(vertices)
    val vertexMap = (0 until vertices.size)
      .map(i => vertices(i) -> i.toLong)
      .toMap
    println(vertexMap)

    // TODO: fare matrice completa
    /*val edgeSet = neighbourJoining.d_i_j
      .filter(_.size >1) // with only one vertex, this is not a path
      .flatMap(list => list.indices.tail.map( i => list(i-1) -> list(i)))
      .map(x => Edge(vertexMap(x._1), vertexMap(x._2), "1"))
      .toSet*/
    //neighbourJoining.d_i_j.foreach(println)
    /*for (el <- distances){
      println(el._1)
      println(el._2)
    }
    println(distances.get((7,1)))
    */

  }
}
