
import scala.collection.immutable.TreeMap


object NGram {
  val NoWindowSize = 0

  def apply[T, U](windowSize: Int = NoWindowSize)(implicit ev: SegmentLike[T, U]): NGram[T, U] =
    new NGram[T, U](ev.emptyPart, 0L, TreeMap.empty[U, NGram[T,U]], windowSize, 0)(ev)

  def apply[T, U](value: U, windowSize: Int = NoWindowSize)(implicit ev: SegmentLike[T, U]): NGram[T, U] =
    new NGram[T, U](value, 1L, TreeMap.empty[U, NGram[T,U]], windowSize, 1)(ev)

  def apply[T, U](value: U, windowSize: Int, maxDepth: Int)(implicit ev: SegmentLike[T, U]): NGram[T, U] =
    new NGram[T, U](value, 1L, TreeMap.empty[U, NGram[T,U]], windowSize, maxDepth)(ev)

  def fromWhole[T, U](whole: T, windowSize: Int = NoWindowSize)(implicit ev: SegmentLike[T, U]): NGram[T, U] = {
    val parts: List[U] = ev.decompose(whole).toList
    val partsWindows: List[List[U]] = windowSize match {
      case NoWindowSize => List(parts)  // no limit; use all parts as one sequence
      case _            => parts.sliding(windowSize).toList
    }

    partsWindows.foldLeft(NGram[T,U](windowSize))((ngSoFar, window) => {
      if (window.size < windowSize && window.last != ev.EndPart)
        throw new Exception(s"Short sequence does not end with EndPart:  ${window.map(_.toString).mkString("<", ">, <", ">")}")

      val partsReverse: List[U] = window.reverse
      val ngWindow = partsReverse.tail.foldLeft(NGram[T,U](partsReverse.head, windowSize, parts.size))((ngSoFar, part) =>
        NGram[T,U](part, windowSize) + ngSoFar
      )
      ngSoFar + ngWindow
    })
  }
}

import NGram._
import scala.collection.immutable.TreeMap

sealed trait Direction
case object Above extends Direction
case object Below extends Direction

case class NGram[T, U](value: U, count: Long, children: TreeMap[U, NGram[T,U]], windowSize: Int, maxDepthPresented: Int)(implicit ev: SegmentLike[T, U]) {
  // a window-size of 1 is degenerate
  require(windowSize == NoWindowSize || windowSize > 1)

  lazy val childSum: Long = (children.map { _ match { case (k, v) => v.count }}).sum

  case class CumulativeChildCount(value: U, cumulativeSum: Long)

  lazy val cumulativeChildCounts: List[CumulativeChildCount] =
  
    children.foldLeft((0L, List[CumulativeChildCount]()))((t, childKV) => {
      val cumulativeSum = t._1 + childKV._2.count
      val newChild = CumulativeChildCount(childKV._1, cumulativeSum)
      (cumulativeSum, t._2 ::: List(newChild))
    })._2

  lazy val nodeCount: Int = 1 + children.map { case (k,v) => v.nodeCount }.toList.sum

  
  override def toString: String =
    s"'${ev.partToString(value)}':$windowSize:$count=${children.map(kv => kv._2.toString).mkString("<", ", ", ">")}"

  
  def prettyPrint(level: Int = 0, ongoing: Map[Int,Boolean] = Map.empty[Int,Boolean]) {
    val leader = (0 until level).map(i => if (ongoing.contains(i)) "|  " else "   ").mkString
    println(s"$leader+- '${ev.partToString(value)}' ($count)")

    (0 until children.size).map(i => {
      val child: NGram[T, U] = children.values.drop(i).take(1).head
      val nextOngoing = if (i < (children.size - 1)) ongoing + ((level + 1) -> true) else ongoing
      child.prettyPrint(level+1, nextOngoing)
    })
  }

  
  def *(factor: Long): NGram[T, U] = {
  
    val newChildren: TreeMap[U, NGram[T,U]] = children.map { case (k,v) => (k, v * factor) }
    this.copy(count = count * factor, children = newChildren)
  }

  // yields the result of adding a new sequence to this n-gram
  def +(whole: T): NGram[T, U] = this + NGram.fromWhole[T, U](whole, this.windowSize)

  // yields the result of adding another n-gram to this one
  def +(that: NGram[T, U]): NGram[T, U] =
    if (that.value == ev.emptyPart) {
      that.children.foldLeft(this)((ngSoFar, childKV) => ngSoFar.blend(childKV._2))
    } else blend(that)

  // assume that you are accumulating what is meant to be a new child
  private def blend(that: NGram[T, U]): NGram[T, U] = {
    
    if (children.contains(that.value)) {
      
      val newChild = if (that.children.size < 1) {
        val oldCount = children(that.value).count
        children(that.value).copy(count = oldCount + that.count)
      }
      else that.children.foldLeft(children(that.value))((childNGSoFar, childKV) => childNGSoFar.blend(childKV._2))
      val newChildren = children + (that.value -> newChild)
      val newCount = newChildren.mapValues(_.count).map(_._2).sum
      this.copy(
        count = newCount, children = newChildren,
        maxDepthPresented = Math.max(maxDepthPresented, that.maxDepthPresented))
    } else {
      // this is a non-peer node not matching any children; just add it
      val newChildren = children + (that.value -> that)
      val newCount = newChildren.mapValues(_.count).map(_._2).sum
      this.copy(
        count = newCount, children = newChildren,
        maxDepthPresented = Math.max(maxDepthPresented, that.maxDepthPresented))
    }
  }

  // probabilistically selects a child based on counts via roulette-wheel selection
  private def randomChild: Option[NGram[T, U]] =
    if (children.size > 0) {
      // roulette-wheel selection
      val x = math.random * childSum
      cumulativeChildCounts.find(_.cumulativeSum >= x)
        .map(cs => children(cs.value))
    } else None

  def getMostFrequent(maxDepth: Int = maxDepthPresented): (List[U], Double) = {
    case class Entry(partial: List[U], frequency: Double) {
      def extend(part: U): Entry = {
        val newPartial = partial ++ List(part)
        val newFrequency = estimateProbability(newPartial)
        Entry(newPartial, newFrequency)
      }
    }

    def recurse(current: Entry, bestSoFar: Option[Entry]): Option[Entry] = {
      if (current.partial.size <= maxDepth) {
        // you can still recurse
        if (current.partial.last != ev.EndPart) {
          // you have not yet terminated, and there are levels of recursion left to use
          if (bestSoFar.isEmpty || current.frequency > bestSoFar.get.frequency) {
            // there's still room for improvement down this chain
            val parent = getLastNode(current.partial)
            parent.children.toList.foldLeft(bestSoFar)((soFar, child) => {
              val childParts = current.partial ++ List(child._1)
              val childFreq = current.frequency * child._2.count.toDouble / parent.count.toDouble
              recurse(Entry(childParts, childFreq), soFar)
            })
          } else bestSoFar
        } else bestSoFar  
      } else {
    
        bestSoFar.map(soFar => if (soFar.frequency > current.frequency) soFar else current).orElse(Option(current))
      }
    }

    recurse(Entry(List(ev.StartPart), 1.0), None) match {
      case Some(entry) => (entry.partial, entry.frequency)
      case None        => (Nil, 0.0)
    }
  }

  def sampleValue: Option[U] = randomChild.map(_.value)

 
  def getLastNode(partsSoFar: List[U]): NGram[T, U] = {
    val lastParts: List[U] =
      if (windowSize == NoWindowSize) partsSoFar
      else partsSoFar.takeRight(windowSize - 1)

    lastParts.foldLeft(Option(this))((parentOptSoFar, part) => parentOptSoFar.flatMap(
      parentSoFar =>
        if (parentSoFar.children.contains(part)) parentSoFar.children.get(part)
        else None
    )).getOrElse({
      lastParts.foldLeft(Option(this))((parentOptSoFar, part) => parentOptSoFar.flatMap(
        parentSoFar =>
          if (parentSoFar.children.contains(part)) parentSoFar.children.get(part)
          else None
      ))
      throw new Exception("Could not find suitable parent for sampling")
    })
  }

  private def nextSamplePart(partsSoFar: List[U]): List[U] = {
    try {
      // identify the bottom-most parent
      val parent = getLastNode(partsSoFar)

      if (parent.children.size < 1) {
        throw new Exception("Parent unexpectedly contains no children:  ")
        parent.prettyPrint()
      }

      // generate a next value
      val nextValue = Option(parent.sampleValue.getOrElse(parent.value))
      if (nextValue.isEmpty) throw new Exception("Could not generate sample value from parent")

      val newParts: List[U] = partsSoFar ++ nextValue
      if (nextValue.get == ev.EndPart) newParts
      else nextSamplePart(newParts)
    } catch {
      case ex: Exception =>
        if (partsSoFar.last == ev.EndPart) partsSoFar
        else throw new Exception("Could not find suitable parent for terminal node of partial sequence:  " +
          partsSoFar.map(_.toString).mkString(", "))
    }
  }

  def sample: T = ev.compose(nextSamplePart(List[U](ev.StartPart)).iterator)

  def estimatePrefixProbability(whole: T): Double =
    estimateProbability(ev.decompose(whole).toList.dropRight(1))

  def estimateProbability(whole: T): Double = estimateProbability(ev.decompose(whole).toList)

  private def estimateProbability(parts: List[U]): Double = {
    // list all of the sequences whose terminal-node probability matters
    val partialTerminals: List[List[U]] =
    // skip the initial START marker probability, and treat it as 1.0
      (2 until math.min(parts.size + 1, windowSize)).map(upperExclusive =>
        parts.slice(0,upperExclusive)).toList
    val windowedTerminals: List[List[U]] = if (parts.size >= windowSize) {
      parts.sliding(windowSize).toList
    } else Nil
    val terminals = partialTerminals ::: windowedTerminals

    def getTerminalProbability(terminalList: List[U]): Double = {
      val (parentCount: Long, child: Option[NGram[T,U]]) = terminalList.foldLeft((0L, Option(this)))((t, value) => t match { case (totalSoFar, ngSoFar) =>
        if (ngSoFar.isDefined && ngSoFar.get.children.contains(value)) (ngSoFar.get.childSum, ngSoFar.get.children.get(value))
        else (0L, None)
      })

      if (parentCount > 0L && child.isDefined) child.get.count.toDouble / parentCount.toDouble
      else 0.0
    }

    terminals.map(terminal => getTerminalProbability(terminal)).product
  }

  def countValues(target: U): Long = {
    val initial: Long = if (value == target) count else 0L

    children.foldLeft(initial)((soFar, child) => soFar + child._2.countValues(target))
  }

  def validate {
    if (value != ev.emptyPart)
      throw new Exception(s"Root node should be empty; instead was:  <$value>}")

    val numStart = countValues(ev.StartPart)
    val numEnd = countValues(ev.EndPart)
    if (numStart != numEnd)
      throw new Exception(s"Mis-matching start, end counts:  $numStart != $numEnd")

    def ensureLength(node: NGram[T, U], remainingDepth: Int) {
      if (remainingDepth < 0)
        throw new Exception(s"Underflow in remainingDepth $remainingDepth")
      if (remainingDepth == 0 && node.children.size > 0)
        throw new Exception(s"Maximum depth exceeded; children = ${node.children.map(_._1.toString).mkString("|")}")
      if (node.children.size < 1 && remainingDepth > 0 && node.value != ev.EndPart)
        throw new Exception(s"Premature termination:  Ends in non-terminal <$node.value> with $remainingDepth layers still expected")
      if (node.children.size > 0)
        node.children.map(child => ensureLength(child._2, remainingDepth - 1))
    }
    children.map(child => ensureLength(child._2, windowSize - 1))
  }

  lazy val numPresentations: Long = countValues(ev.StartPart)

  lazy val numUniquePresentations: Long =
    children.get(ev.StartPart).map(_.children.size.toLong).getOrElse(0L)

  lazy val numTerminals: Long = {
    Math.max(1L, children.foldLeft(0L)((tSoFar, child) => tSoFar + child._2.numTerminals))
  }

  private def _sequenceIterator: Iterator[Seq[U]] = new Iterator[Seq[U]] {
    var index = 0
    var childItr: Option[Iterator[NGram[T,U]]] =
      if (children.size == 0) None
      else Option(children.valuesIterator)
    var source: Iterator[Seq[U]] = nextSource()

    def nextSource(): Iterator[Seq[U]] = childItr match {
      case None => Seq(Seq()).iterator
      case Some(ngramItr) => ngramItr.next()._sequenceIterator
    }

    def hasNext: Boolean = index == 0 || index < numTerminals

    def next(): Seq[U] = {
      // sanity check
      if (!hasNext) throw new Exception("Iterator exhausted.")

      index = index + 1

      if (source.hasNext) Seq(value) ++ source.next()
      else {
        source = nextSource()
        if (source.hasNext) Seq(value) ++ source.next()
        else throw new Exception(s"Unexpected iterator exhaustion")
      }
    }
  }

  
  def sequenceIterator: Iterator[Seq[U]] = _sequenceIterator.map(_.tail)


  def sequenceAssociationCounts(seq: Seq[U]): List[Int] = {
    val seqItr: Iterator[Seq[U]] = sequenceIterator

    val thisSeqEnd = seq.tail
    val thisSeqStart = seq.dropRight(1)
    seqItr.map(otherSeq => {
      if (seq != otherSeq && (thisSeqEnd == otherSeq.dropRight(1) || otherSeq.tail == thisSeqStart)) 1
      else 0
    }).toList
  }

  
  def sequenceAssociationMatrixRowIterator: Iterator[Seq[Int]] = {
    val seqItr: Iterator[Seq[U]] = sequenceIterator

    seqItr.map(seq => sequenceAssociationCounts(seq))
  }

  
  def numBelow(whole: T): Long =
    numDirection(ev.decompose(whole).toList, Below)

  def numAbove(whole: T): Long =
    numDirection(ev.decompose(whole).toList, Above)

  private def numDirection(parts: List[U], direction: Direction): Long = {
    
    val windows: List[List[U]] = parts.sliding(windowSize).toList

    def inDirection(a: U, b: U): Boolean = direction match {
      case Above => ev.compare(a, b) > 0
      case Below => ev.compare(a, b) < 0
    }

    // how to count:  whether only by uniques (default) or by distribution
    val kvCountFnx: ((U, NGram[T,U])) => Long = (t) => t._2.count

    def _getDirection(ng: NGram[T,U], window: List[U]): Long = {
      if (window.size < 1) 0L
      else {
        if (!ng.children.contains(window.head))
          ng.children.filter(child => inDirection(child._1, window.head)).map(kvCountFnx).sum
        else {
          val ofChildren = ng.children.filter(child => inDirection(child._1, window.head)).map(kvCountFnx).sum
          val recurse = _getDirection(ng.children(window.head), window.tail)

          //@TODO debug!
          //println(s"  [MATCH] $direction ng $ng window $window ofChildren $ofChildren recurse $recurse ")

          ofChildren + recurse
        }
      }
    }

   
    val fullWindows = windows.filter(_.size == windowSize)
    val numFull: Long = fullWindows.foldLeft((0L, true))((t, fullWindow) => {
      val (numSoFar, valid) = t

      if (valid) {
        val (numFullLocal: Long, stillValid: Boolean) =
          if (children.contains(fullWindow.head)) {
            val grandchildren: Map[U, NGram[T,U]] = children(fullWindow.head).children
            (grandchildren.filter(grandchild => inDirection(grandchild._1, fullWindow.drop(1).head)).map(kvCountFnx).sum,
              grandchildren.contains(fullWindow.drop(1).head))
          } else (0L, false)

        //println(s"  [$direction FULL ${if (valid) "valid" else "INVALID"}] transition [${fullWindow.head}, ${fullWindow.drop(1).head}] -> $numSoFar + $numFullLocal")

        (numSoFar + numFullLocal, valid && stillValid)
      } else (numSoFar, false)
    })._1


    val partialWindows: List[List[U]] = windows.filter(_.size < windowSize)
    val numPartial: Long = partialWindows.foldLeft(0L)((numPartialSoFar, partialWindow) => {
      numPartialSoFar +
        (if (children.contains(partialWindow.head))
          _getDirection(children(partialWindow.head), partialWindow.tail)
        else 0L)
    })

    
    numFull + numPartial
  }

  def getRelativePosition(whole: T): Long = {
    val below: Long = numBelow(whole)
    val above: Long = numAbove(whole)
    val total: Long =numPresentations

    below + total - above
  }

  def getLexicalDifference(a: T, b: T): Double = {
    val total: Long = numPresentations
    0.5 *
      Math.abs(getRelativePosition(a) - getRelativePosition(b)).toDouble /
      total.toDouble
  }


  lazy val probePrecision: Int = Math.ceil(Math.log(numPresentations << 1L) / Math.log(2.0)).toInt

  def getRelativePositionBits(whole: T): Seq[Boolean] = {
    val index = getRelativePosition(whole)
    val bitString = index.toBinaryString.reverse.padTo(probePrecision, '0').reverse
    bitString.map(_ == '1')
  }
