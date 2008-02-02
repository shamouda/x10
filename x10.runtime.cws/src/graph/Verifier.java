package graph;
import x10.runtime.cws.Frame;
import x10.runtime.cws.Job;
import x10.runtime.cws.Pool;
import x10.runtime.cws.StealAbort;
import x10.runtime.cws.Worker;
import x10.runtime.cws.Job.GloballyQuiescentVoidJob;

public final class Verifier extends Frame {
		final int start, end; // both inclusive
		final Vertex[] G;
		boolean[] reachesRoot;
		BooleanRef result;
		public Verifier(Vertex[] G, int s, int e, boolean[] rr, BooleanRef r) { 
			start=s; end=e; this.G=G;
			this.reachesRoot=rr;
			this.result=r;
			//System.out.println("Created verifier " + s + " " + e);
			}
		public void compute(Worker w) {
			w.popAndReturnFrame();
			for (int i=start; i <= end; i++) 
				if (! this.G[i].verify(this.G[1], reachesRoot, G)) {
					result.value=false;
					return;
				}
		}
		public static final long NPS = 1000*1000*1000;
		public static void verify(Pool g, final Vertex[] G, final boolean[] rr, final BooleanRef r) {
			assert G.length==rr.length;
			long verificationTime = - System.nanoTime();
			int N = G.length;
			final int P=g.getPoolSize(), size = N/P;
			//System.out.println("verify: P=" + P + " N=" + N);
			r.value=true;
			for (int j=0; j < rr.length; j++) rr[j]=false;
			Job vJob = new GloballyQuiescentVoidJob(g, 
					new Frame() {
				@Override public void compute(Worker w) throws StealAbort {
					w.popAndReturnFrame();
					for (int j=0; j < P; j++) 
						w.pushFrame(new Verifier(G, j*size, (j+1)*size-1,rr,r));
				}
			});
			g.invoke(vJob);
			if (! r.value)
				System.out.println(" false! ");
			else {
				verificationTime += System.nanoTime();
				double vSecs = ((double) verificationTime)/NPS;
				System.out.printf(" (verification %5.2f s)", vSecs );
				System.out.println();
			}
		}
	}