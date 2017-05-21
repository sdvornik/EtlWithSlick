package com.yahoo.sdvornik.db;

public class IndexSearcherEntryPoint {

  public static void main(String[] args) {
    Integer[] probe = new Integer[] {4, 5, 25, 76, 120};
    IndexSearcher<Integer> searcher = new IndexSearcher<>(probe);
    System.out.println(searcher.binarySearch(3));
  }

}
