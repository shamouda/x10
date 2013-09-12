package futuresched.benchs.conncomp;

import x10.util.Random;
import x10.util.ArrayList;


public class FutGraph {

   public var nodes: ArrayList[FutNode];

   public def this(nodes: ArrayList[FutNode]) {
      this.nodes = nodes;
   }

   // A random connected undirected graph with n nodes and mb maximum branching factor.
   public static def random(n: Int, mb: Int): FutGraph {
      val list = new ArrayList[FutNode]();
      for (var i: Int = 0; i < n; i++)
         list.add(new FutNode(i));

//      for (var i: Int = n - 1; i >= 1 ; i--) {
//         val node1 = list.get(i);
//         val node2 = list.get(i-1);
//         node1.addNeighbor(node2);
//         node2.addNeighbor(node1);
//      }

      val nRand = new Random();
      val bRand = new Random();

      for (var i: Int = 0; i < n/2; i++) {
         val node1 = list.get(i);
         val b = 1 + bRand.nextInt(mb);
         //Console.OUT.println("branching: " + b);
         var d1: Int = node1.degree();
         while (d1 < b) {
            val node2Index = nRand.nextInt(n/2);
            val node2 = list.get(node2Index);
            if (node1 != node2 && !node1.contains(node2) && node2.degree() < mb) {
               node1.addNeighbor(node2);
               node2.addNeighbor(node1);
               d1 = node1.degree();
            }
         }
      }
      for (var i: Int = n/2; i < n; i++) {
         val node1 = list.get(i);
         val b = 1 + bRand.nextInt(mb);
         //Console.OUT.println("branching: " + b);
         var d1: Int = node1.degree();
         while (d1 < b) {
            val node2Index = n/2 + nRand.nextInt(n/2);
            val node2 = list.get(node2Index);
            if (node1 != node2 && !node1.contains(node2) && node2.degree() < mb) {
               node1.addNeighbor(node2);
               node2.addNeighbor(node1);
               d1 = node1.degree();
            }
         }
      }

      return new FutGraph(list);
   }

   public def toString(): String {

      var s: String = "graph {\n";
      val iter = nodes.iterator();
      while (iter.hasNext()) {
         val node = iter.next();
         //s += node.no + ": ";
         val iter2 = node.neighbors.iterator();
         while (iter2.hasNext()) {
            val node2 = iter2.next();
            if (node.no <= node2.no)
               s += "\t" + node.no + " -- " + node2.no + ";\n";
         }
         //s += "\n";
      }
      s += "}\n";
      return s;
   }

   public def toStringComps(): String {

      var s: String = "Comp:\n";
      val iter = nodes.iterator();
      while (iter.hasNext()) {
         val node = iter.next();
         //s += node.no + ": ";
         s += "\t" + node.no + ": " + node.comp.get() + "\n";
      }
      return s;
   }


}





