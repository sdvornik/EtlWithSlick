package com.yahoo.sdvornik.util

class IndexSearcher[T : Ordering](array: Array[T]) {

  def binarySearch(key: T)(implicit ord: Ordering[T]): Int = {
    var low = 0
    var high = array.length - 1
    while ( {
      low <= high
    }) {
      val mid = (low + high) >>> 1
      val cmp = ord.compare(array(mid),key)
      if (cmp < 0) low = mid + 1
      else if (cmp > 0) high = mid - 1
      else return mid
    }
    if (low > array.length - 1) return array.length - 1
    low - 1
  }
}
