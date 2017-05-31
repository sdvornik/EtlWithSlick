package com.yahoo.sdvornik.db.tuples;

public final class AmWkKey {

  private final int indx;

  private final String grade;

  public AmWkKey(int indx, String grade) {
    this.indx = indx;
    this.grade = grade;
  }

  public int getIndx() {
    return indx;
  }

  public String getGrade() {
    return grade;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AmWkKey amWkKey = (AmWkKey) o;

    return (indx == amWkKey.indx) && (grade.equals(amWkKey.grade));
  }

  @Override
  public int hashCode() {
    return 31 * indx + grade.hashCode();
  }
}
