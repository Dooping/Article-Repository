package cp.articlerep;

public class MainRep {

	public static final boolean DO_VALIDATION = Boolean.parseBoolean(System
			.getProperty("cp.articlerep.validate"));

	
	public static void main(String[] args) {

		if (args.length < 9) {
			System.out
					.println("usage: "
							+ MainRep.class.getCanonicalName()
							+ " time(sec) nthreads nkeys put(%) del(%) get(%) nauthors nkeywords nfindlist");
			System.exit(1);
		}

		int time = Integer.parseInt(args[0]);
		int nthreads = Integer.parseInt(args[1]);
		int nkeys = Integer.parseInt(args[2]);
		int put = Integer.parseInt(args[3]);
		int del = Integer.parseInt(args[4]);
		int get = Integer.parseInt(args[5]);
		
		if (put + del + get != 100) {
			System.out.println("Error: "
					+ " put(%) + del(%) + get(%) must add to 100%");
			System.exit(1);
		}
		int nauthors = Integer.parseInt(args[6]);
		int nkeywords = Integer.parseInt(args[7]);
		int nfindlist = Integer.parseInt(args[8]);

		Worker run = new Worker(nkeys, "resources/dictionary.txt", put, del,
				get, nauthors, nkeywords, nfindlist);

		run.spawnThread(nthreads);

		run.startTest();

		long start_time = System.currentTimeMillis();

		try {
			if (!DO_VALIDATION) {
				Thread.sleep(time * 1000);
			} else {
				for (int i = 0; i < time; i++) {
					Thread.sleep(1000);

					if (i % 5 == 0) {
						run.pauseTest();

						System.out.println("[" + i
								+ "] -----------VALIDATION-----------");

						if (!run.getRepository().validate() || !run.validateOps()) {
							System.out.println("[VALIDATION ERROR]");
							run.stopTest();
							return;

						}

						run.restartTest();

						System.out.println("Check done");
					}

				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		run.stopTest();

		long end_time = System.currentTimeMillis();

		run.joinThreads();

		System.out.println("Total time: " + ((end_time - start_time) / 1000) + " seconds");
		System.out.println("Operation rate: "
				+ Math.round(run.getTotalOperations()
						/ ((end_time - start_time) / 1000.0)) + " ops/s");
	}

}
