package com.yahoo.sdvornik.db;


public class IndexSearcher <T extends Comparable> {

  private T[] array;

  public IndexSearcher(T[] array) {
    this.array = array;
  }

  public int binarySearch(T key) {
    int low = 0;
    int high = array.length - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int cmp = array[mid].compareTo(key);

      if (cmp < 0) low = mid + 1;
      else if (cmp > 0) high = mid - 1;
      else return mid;
    }
    if(low > array.length - 1) return array.length -1;
    return low-1;
  }
}
