package com.yahoo.sdvornik.db.toh_input;

public class PreTohInput {
 private final Integer debut;
 private final Integer too;
 private final Integer mdStart;
 private final Integer exit;
 private final Integer numSizes;
 private int tohCalc;
 private int uncFcst;
 private int toh;

 public PreTohInput(
   int tohCalc,
   int uncFcst
 ) {
   this.debut = null;
   this.too = null;
   this.mdStart = null;
   this.exit = null;
   this.numSizes = null;
   this.tohCalc = tohCalc;
   this.uncFcst = uncFcst;
   this.toh = 0;
 }

  public PreTohInput(int debut, int mdStart, int too, int exit) {
    this.debut = debut;
    this.too = too;
    this.mdStart = mdStart;
    this.exit = exit;
    this.numSizes = 0;
    this.tohCalc = 0;
    this.uncFcst = 0;
    this.toh = 0;
  }

  public int getDebut() {
    return debut;
  }

  public int getToo() {
    return too;
  }

  public int getMdStart() {
    return mdStart;
  }

  public int getExit() {
    return exit;
  }

  public int getNumSizes() {
    return numSizes;
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

  public void setTohCalc(int tohCalc) {
    this.tohCalc = tohCalc;
  }

  public void setUncFcst(int uncFcst) {
    this.uncFcst = uncFcst;
  }

  public void setToh(int toh) {
    this.toh = toh;
  }
}
