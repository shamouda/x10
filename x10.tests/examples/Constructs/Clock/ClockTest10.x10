/*
 *
 * (C) Copyright IBM Corporation 2006
 *
 *  This file is part of X10 Test.
 *
 */
import harness.x10Test;

/**
 * Using clocks to do simple producer consumer synchronization
 * for this task DAG (arrows point downward)
 * in pipelined fashion. On each clock period,
 * each stage of the pipeline reads the previous clock period's
 * result from the previous stage and produces its new result
 * for the current clock period.
 *
 * <code>
 *   A   stage 0 A produces the stream 1,2,3,...
 *  / \
 *  B  C  stage 1 B is "double", C is "square" function
 *  \ /|
 *   D E  stage 2 D is \(x,y)(x+y+10), E is \(x)(x*7)
 * </code>
 *
 * @author kemal 4/2005
 */
public class ClockTest10 extends x10Test {
    val varA = Rail.makeVar[int](2,(x:nat)=>0);
    val varB = Rail.makeVar[int](2,(x:nat)=>0);
    val varC = Rail.makeVar[int](2,(x:nat)=>0);
    val varD = Rail.makeVar[int](2,(x:nat)=>0);
    val varE = Rail.makeVar[int](2,(x:nat)=>0);
    public const N: int = 10;
    public const pipeDepth: int = 2;

    static def ph(var x: int): int = { return x % 2; }

    public def run(): boolean = {
	finish async(here) {
	    val a: Clock = Clock.make();
	    val b: Clock = Clock.make();
	    val c: Clock = Clock.make();
	    async clocked(a) taskA(a);
	    async clocked(a, b) taskB(a, b);
	    async clocked(a, c) taskC(a, c);
	    async clocked(b, c) taskD(b, c);
	    async clocked(c) taskE(c);
	}
	return true;
    }

    def taskA(val a: Clock): void = {
	for ((k):Point(1) in 1..N) {
	    varA(ph(k)) = k;
	    x10.io.Console.OUT.println( " " + k + " A producing " + varA(ph(k)));
	    next;
	}
    }
    def taskB(val a: Clock, val b: Clock): void = {
	for ((k):Point(1) in 1..N) {
	    val tmp = new boxedInt();
	    finish tmp.val = varA(ph(k-1))+varA(ph(k-1));
	    x10.io.Console.OUT.println(" " + k + " B consuming oldA producing " + tmp.val);
	    a.resume();
	    varB(ph(k)) = tmp.val;
	    x10.io.Console.OUT.println(" " + "B before next");
	    next;
	}
    }
    def taskC(val a: Clock, val c: Clock): void = {
	for ((k):Point(1) in 1 ..N) {
	    val tmp: boxedInt = new boxedInt();
	    finish tmp.val = varA(ph(k-1))*varA(ph(k-1));
	    x10.io.Console.OUT.println(" " + k + " C consuming oldA "+ tmp.val);
	    a.resume();
	    varC(ph(k)) = tmp.val;
	    x10.io.Console.OUT.println(" " + "C before next");
	    next;
	}
    }
    def taskD(val b: Clock, val c: Clock): void = {
	for ((k):Point(1) in 1 ..N) {
	    val tmp: boxedInt = new boxedInt();
	    finish tmp.val = varB(ph(k-1))+varC(ph(k-1))+10;
	    x10.io.Console.OUT.println(" " + k + " D consuming oldB+oldC producing " + tmp.val);
	    c.resume();
	    b.resume();
	    varD(ph(k)) = tmp.val;
	    x10.io.Console.OUT.println(" " + k + " D before next");
	    var n: int = k-pipeDepth;
	    chk(!(k>pipeDepth) || varD(ph(k)) == n+n+n*n+10);
	    next;
	}
    }
    def taskE(val c: Clock): void = {
	for ((k):Point(1) in 1 ..N) {
	    val tmp: boxedInt = new boxedInt();
	    finish tmp.val = varC(ph(k-1))*7;
	    x10.io.Console.OUT.println(" " + k + " E consuming oldC producing " + tmp.val);
	    c.resume();
	    varE(ph(k)) = tmp.val;
	    x10.io.Console.OUT.println(" " + k + " E before next");
	    var n: int = k-pipeDepth;
	    chk(!(k>pipeDepth) || varE(ph(k)) == n*n*7);
	    next;
	}
    }

    public static def main(var args: Rail[String]): void = {
	new ClockTest10().execute();
    }

    static class boxedInt {
	var val: int;
    }
}
