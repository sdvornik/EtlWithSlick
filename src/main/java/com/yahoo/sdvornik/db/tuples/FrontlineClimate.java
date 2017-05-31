package com.yahoo.sdvornik.db.tuples;

public final class FrontlineClimate {

  private final String grade;

  private final String strClimate;

  public FrontlineClimate(String grade, String strClimate) {
    this.grade = grade;
    this.strClimate = strClimate;
  }

  public String getGrade() {
    return grade;
  }

  public String getStrClimate() {
    return strClimate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FrontlineClimate that = (FrontlineClimate) o;

    return grade.equals(that.grade) && strClimate.equals(that.strClimate);
  }

  @Override
  public int hashCode() {
    return 31 * grade.hashCode() + strClimate.hashCode();
  }
}
