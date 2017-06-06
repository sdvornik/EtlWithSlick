package com.yahoo.sdvornik.db.toh_input;

import com.yahoo.sdvornik.db.tuples.TooNumSizesKey;

public class TohInput {

  public final static TohInput Default = new TohInput();

  private final TooNumSizesKey tooNumSizesKey;
  private final int tohCalc;

  private int uncFcst = 0;
  private int toh = 0;

  public TohInput(PreTohInput preTohInput, int tohCalc) {
    this.tooNumSizesKey = preTohInput.getTooNumSizesKey();
    this.tohCalc = tohCalc;
  }

  private TohInput() {
    this.tooNumSizesKey = null;
    this.tohCalc = 0;
  }

  public TooNumSizesKey getTooNumSizesKey() {
    return tooNumSizesKey;
  }

  public int getTohCalc() {
    return tohCalc;
  }

  public int getUncFcst() {
    return uncFcst;
  }

  public int getToh() {
    return toh;
  }

  public void setUncFcst(int uncFcst) {
    this.uncFcst = uncFcst;
  }

  public void setToh(int toh) {
    this.toh = toh;
  }
}
