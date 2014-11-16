package example

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 11/16/14
 * Time: 6:47 PM
 */

object SentenceValidation {

  def isEditDistance1(s1: Array[String], s2: Array[String]): Boolean = {
    if (s1.length == s2.length)
      isSubstitutionValid(s1 zip s2)
    if (s1.length > s2.length)
      isAdditionDeletionValid(s1, s2)
    else
      isAdditionDeletionValid(s2, s1)
  }

  def isSubstitutionValid(p: Array[(String, String)], acc: Int = 0): Boolean = {
    p match {
      case Array(x, xs@_*) =>
        if (x._1 equals x._2)
          isSubstitutionValid(xs.toArray)
        else {
          if (acc + 1 == 2) false
          else isSubstitutionValid(xs.toArray, 1)
        }
      case Array() => true
    }
  }

  def isAdditionDeletionValid(long: Array[String], short: Array[String], acc: Int = 0): Boolean = {
    (long, short) match {
      case (l, s) if s.size == 0 => true
      case (l, s) if l.head equals s.head => isAdditionDeletionValid(l.tail, s.tail)
      case (l, s) if !l.head.equals(s.head) => l.tail zip s forall (p => p._1 equals p._2)
    }
  }
}
