package cp.articlerep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import cp.articlerep.ds.Iterator;
import cp.articlerep.ds.LinkedList;
import cp.articlerep.ds.List;

public class Worker {

	public static final boolean DO_VALIDATION = Boolean.parseBoolean(System
			.getProperty("cp.articlerep.validate"));

	private int dictSize;
	private String dictFile;
	private int put;
	private int del;
	private int get;
	private int authors;
	private int keywords;
	private int findList;

	private String[] wordArray;
	private Thread[] workers;
	private Job[] jobs;

	private Repository repository;

	private volatile boolean running;
	private volatile boolean pause;

	private int totalOperations;

	/**
	 * @param dictSize
	 * @param dictFile
	 * @param put
	 * @param del
	 * @param get
	 * @param authors
	 * @param keywords
	 * @param findList
	 */
	public Worker(int dictSize, String dictFile, int put, int del, int get,
			int authors, int keywords, int findList) {
		this.dictSize = dictSize;
		this.dictFile = dictFile;
		this.put = put;
		this.del = del;
		this.get = get;
		this.authors = authors;
		this.keywords = keywords;
		this.findList = findList;

		populateWordArray();

		this.workers = null;
		this.jobs = null;

		this.repository = new Repository(dictSize);

		this.running = true;
		this.pause = true;

		this.totalOperations = 0;

	}

	public Repository getRepository() {
		return this.repository;
	}

	private void populateWordArray() {
		wordArray = new String[dictSize];

		try {
			BufferedReader br = new BufferedReader(
					new FileReader(this.dictFile));
			String line;
			int i = 0;

			while ((line = br.readLine()) != null && i < dictSize) {
				wordArray[i] = line;
				i++;
			}

			if (i < dictSize) {
				dictSize = i;
			}

			br.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}

	}

	private synchronized void updateOperations(int operations) {
		this.totalOperations += operations;
	}

	public synchronized int getTotalOperations() {
		return totalOperations;
	}

	public class Job implements Runnable {

		private int put;
		private int del;
		private int get;
		private int count;
		private int putCount;
		private int delCount;
		private int getCount;
		private Random rand;
		private volatile boolean paused;

		/**
		 * @param put percentage of insert article operations
		 * @param del percentage of remove article operations
		 * @param get percentage of find article operations, which is 
		 * 			  shared by findByAuthor and findByKeyword
		 */
		public Job(int put, int del, int get) {
			this.put = put;
			this.del = del;
			this.get = get;
			this.count = 0;
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
			this.rand = new Random(System.nanoTime());
			paused = true;
		}
		
		public int getCount() {
			return count;
		}

		public int getPutCount() {
			return putCount;
		}

		public int getDelCount() {
			return delCount;
		}

		public int getGetCount() {
			return getCount;
		}

		private boolean contains(List<String> list, String word) {
			Iterator<String> it = list.iterator();
			while (it.hasNext()) {
				if (it.next().compareTo(word) == 0)
					return true;
			}
			return false;
		}

		private Article generateArticle() {
			int i = rand.nextInt(dictSize);
			Article a = new Article(i, wordArray[i]);

			int nauthors = authors;
			while (nauthors > 0) {
				int p = rand.nextInt(dictSize);
				String word = wordArray[p];
				if (!contains(a.getAuthors(), word)) {
					a.addAuthor(word);
					nauthors--;
				}
			}

			int nkeywords = keywords;
			while (nkeywords > 0) {
				int p = rand.nextInt(dictSize);
				String word = wordArray[p];
				if (!contains(a.getKeywords(), word)) {
					a.addKeyword(word);
					nkeywords--;
				}
			}

			return a;
		}

		private List<String> generateListOfWords() {
			List<String> res = new LinkedList<String>();
			int nwords = findList;

			while (nwords > 0) {
				int p = rand.nextInt(dictSize);
				String word = wordArray[p];
				if (!contains(res, word)) {
					res.add(word);
					nwords--;
				}
			}

			return res;
		}

		public void run() {

			while (pause) {
				Thread.yield();
			}

			paused = true;

			while (running) {

				if (DO_VALIDATION) {
					boolean done = false;

					while (pause) {
						if (!done) {
							System.out.println("Thread "
									+ Thread.currentThread().getId()
									+ ": Stoped");
							paused = true;
							done = true;
						}
					}
					paused = false;
				}

				int op = rand.nextInt(100);


				if (op < put) {
					Article a = generateArticle();
					if(repository.insertArticle(a)){
						putCount++;
						count++;
					}
					
				} else if (op < put + del) {
					int id = rand.nextInt(dictSize);
					if(repository.removeArticle(id)){
						delCount++;
						count++;
					}
				} else if (op < put + del + (get / 2)) {
					List<String> list = generateListOfWords();
					repository.findArticleByAuthor(list);
					getCount++;
					count++;
				} else {
					List<String> list = generateListOfWords();
					repository.findArticleByKeyword(list);
					getCount++;
					count++;
				}


			}

			updateOperations(count);

		}

	}

	public void spawnThread(int nthreads) {
		workers = new Thread[nthreads];
		jobs = new Job[nthreads];

		for (int i = 0; i < nthreads; i++) {
			jobs[i] = new Job(put, del, get);
			workers[i] = new Thread(jobs[i]);
		}

		for (int i = 0; i < nthreads; i++) {
			workers[i].start();
		}

	}

	public void joinThreads() {
		for (int i = 0; i < workers.length; i++) {
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void startTest() {
		this.running = true;
		this.pause = false;
	}

	public void stopTest() {
		this.running = false;
		this.pause = false;
	}

	public void pauseTest() {
		this.pause = true;

		while (true) {
			boolean ok = true;

			for (int i = 0; i < jobs.length; i++)
				if (!jobs[i].paused) {
					ok = false;
					break;
				}

			if (ok)
				break;
		}

	}

	public void restartTest() {
		this.pause = false;
	}
	
	public boolean validateOps(){
		int totalPut = 0;
		int totalDel = 0;
		int totalGet = 0;
		int totalCount = 0;
		for(Job job: jobs){
			totalPut += job.getPutCount();
			totalDel += job.getDelCount();
			totalGet += job.getGetCount();
			totalCount += job.getCount();
		}
		if(totalPut < totalDel)
			return false;
		
		else if(totalCount != totalDel + totalGet + totalPut)
			return false;
		
		return true;
	}
}
