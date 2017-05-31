package com.yahoo.sdvornik.db.toh_input;

import com.yahoo.sdvornik.db.tuples.TooNumSizesKey;

import java.util.Arrays;
import java.util.Objects;

public final class PreTohInput {

  public final static PreTohInput Default = new PreTohInput();

  private final Integer debut;
  private final Integer mdStart;
  private final Integer exit;
  private final TooNumSizesKey tooNumSizesKey;


  private PreTohInput() {
    this.debut = null;
    this.mdStart = null;
    this.exit = null;
    this.tooNumSizesKey = null;
  }

  public PreTohInput(int debut, int mdStart, int exit, int too, int numSizes) {
    this.debut = debut;
    this.mdStart = mdStart;
    this.exit = exit;
    this.tooNumSizesKey = new TooNumSizesKey(too, numSizes);

  }

  public int getDebut() {
    return debut;
  }

  public int getMdStart() {
    return mdStart;
  }

  public int getExit() {
    return exit;
  }

  public TooNumSizesKey getTooNumSizesKey() {
    return tooNumSizesKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PreTohInput that = (PreTohInput) o;
    return Objects.equals(debut, that.debut) &&
      Objects.equals(mdStart, that.mdStart) &&
      Objects.equals(exit, that.exit) &&
      Objects.equals(tooNumSizesKey, that.tooNumSizesKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(debut, mdStart, exit, tooNumSizesKey);
  }
}
