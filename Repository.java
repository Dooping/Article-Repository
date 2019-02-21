package cp.articlerep;

import java.util.ArrayList;
import java.util.HashSet;

import cp.articlerep.ds.Iterator;
import cp.articlerep.ds.LinkedList;
import cp.articlerep.ds.List;
import cp.articlerep.ds.Map;
import cp.articlerep.ds.HashTable;
import java.util.concurrent.locks.*;

/**
 * @author Ricardo Dias
 */
public class Repository {

	private Map<String, List<Article>> byAuthor;
	private Map<String, List<Article>> byKeyword;
	private Map<Integer, Article> byArticleId;
	private Map<String, Lock> byAuthorLocks;
	private Map<String, Lock> byKeywordLocks;
	private ArrayList<Lock> byArticleLocks;

	public Repository(int nkeys) {
		this.byAuthor = new HashTable<String, List<Article>>(40000);
		this.byKeyword = new HashTable<String, List<Article>>(40000);
		this.byArticleId = new HashTable<Integer, Article>(40000);
		this.byAuthorLocks = new HashTable<String, Lock>(40000);
		this.byKeywordLocks = new HashTable<String, Lock>(40000);
		this.byArticleLocks = new ArrayList<Lock>(40000);
		for(int i = 0; i<40000; i++)
			this.byArticleLocks.add(new ReentrantLock());
	}

	public boolean insertArticle(Article a) {
		byArticleLocks.get(a.getId()).lock();

		if (byArticleId.contains(a.getId())){
			byArticleLocks.get(a.getId()).unlock();
			return false;
		}

		Iterator<String> authors = a.getAuthors().iterator();
		while (authors.hasNext()) {
			String name = authors.next();

			Lock l = byAuthorLocks.get(name);
			if(l==null){
				l = new ReentrantLock();
				byAuthorLocks.put(name, l);
			}
			l.lock();
			List<Article> ll = byAuthor.get(name);
			if (ll == null) {
				ll = new LinkedList<Article>();
				byAuthor.put(name, ll);
			}
			ll.add(a);
			l.unlock();
		}

		Iterator<String> keywords = a.getKeywords().iterator();
		while (keywords.hasNext()) {
			String keyword = keywords.next();

			Lock l = byKeywordLocks.get(keyword);
			if(l==null){
				l = new ReentrantLock();
				byKeywordLocks.put(keyword, l);
			}
			l.lock();
			List<Article> ll = byKeyword.get(keyword);
			if (ll == null) {
				ll = new LinkedList<Article>();
				byKeyword.put(keyword, ll);
			} 
			ll.add(a);
			l.unlock();
		}

		byArticleId.put(a.getId(), a);
		byArticleLocks.get(a.getId()).unlock();

		return true;
	}

	public boolean removeArticle(int id) {
		byArticleLocks.get(id).lock();
		Article a = byArticleId.get(id);

		if (a == null){
			byArticleLocks.get(id).unlock();
			return false;
		}
		
		a = byArticleId.remove(id);

		Iterator<String> keywords = a.getKeywords().iterator();
		while (keywords.hasNext()) {
			String keyword = keywords.next();
			
			Lock l = byKeywordLocks.get(keyword);
			l.lock();

			List<Article> ll = byKeyword.get(keyword);
			if (ll != null) {
				int pos = 0;
				Iterator<Article> it = ll.iterator();
				while (it.hasNext()) {
					Article toRem = it.next();
					if (toRem == a) {
						break;
					}
					pos++;
				}
				ll.remove(pos);
				it = ll.iterator();
				if (!it.hasNext()) { // checks if the list is empty
					byKeyword.remove(keyword);
				}
			}
			l.unlock();
		}

		Iterator<String> authors = a.getAuthors().iterator();
		while (authors.hasNext()) {
			String name = authors.next();
			
			Lock l = byAuthorLocks.get(name);
			l.lock();

			List<Article> ll = byAuthor.get(name);
			if (ll != null) {
				int pos = 0;
				Iterator<Article> it = ll.iterator();
				while (it.hasNext()) {
					Article toRem = it.next();
					if (toRem == a) {
						break;
					}
					pos++;
				}
				ll.remove(pos);
				it = ll.iterator(); 
				if (!it.hasNext()) { // checks if the list is empty
					byAuthor.remove(name);
				}
			}
			l.unlock();
		}
		byArticleLocks.get(id).unlock();
		return true;
	}

	public List<Article> findArticleByAuthor(List<String> authors) {
		List<Article> res = new LinkedList<Article>();

		Iterator<String> it = authors.iterator();
		while (it.hasNext()) {
			String name = it.next();
			Lock l = byAuthorLocks.get(name);
			if(l!=null)
				l.lock();
			List<Article> as = byAuthor.get(name);
			if (as != null) {
				Iterator<Article> ait = as.iterator();
				while (ait.hasNext()) {
					Article a = ait.next();
					res.add(a);
				}
			}
			if(l!=null)
				l.unlock();
		}

		return res;
	}

	public List<Article> findArticleByKeyword(List<String> keywords) {
		List<Article> res = new LinkedList<Article>();

		Iterator<String> it = keywords.iterator();
		while (it.hasNext()) {
			String keyword = it.next();
			Lock l = byKeywordLocks.get(keyword);
			if(l!=null)
				l.lock();
			List<Article> as = byKeyword.get(keyword);
			if (as != null) {
				Iterator<Article> ait = as.iterator();
				while (ait.hasNext()) {
					Article a = ait.next();
					res.add(a);
				}
			}
			if(l!=null)
				l.unlock();
		}

		return res;
	}

	
	/**
	 * This method is supposed to be executed with no concurrent thread
	 * accessing the repository.
	 * 
	 */
	public boolean validate() {
		
		HashSet<Integer> articleIds = new HashSet<Integer>();
		int articleCount = 0;
		
		Iterator<Article> aIt = byArticleId.values();
		while(aIt.hasNext()) {
			Article a = aIt.next();
			
			articleIds.add(a.getId());
			articleCount++;
			
			// check the authors consistency
			Iterator<String> authIt = a.getAuthors().iterator();
			while(authIt.hasNext()) {
				String name = authIt.next();
				if (!searchAuthorArticle(a, name)) {
					return false;
				}
			}
			
			// check the keywords consistency
			Iterator<String> keyIt = a.getKeywords().iterator();
			while(keyIt.hasNext()) {
				String keyword = keyIt.next();
				if (!searchKeywordArticle(a, keyword)) {
					return false;
				}
			}
		}
		
		Iterator<String> authIt = byAuthor.keys();
		while(authIt.hasNext())
			if(!searchArticleAuthor(authIt.next()))
				return false;
		
		Iterator<String> keyIt = byKeyword.keys();
		while(keyIt.hasNext())
			if(!searchArticleKeyword(keyIt.next()))
				return false;
		
		return articleCount == articleIds.size();
	
	}
	
	private boolean searchAuthorArticle(Article a, String author) {
		List<Article> ll = byAuthor.get(author);
		if (ll != null) {
			Iterator<Article> it = ll.iterator();
			while (it.hasNext()) {
				if (it.next() == a) {
					return true;
				}
			}
		}
		return false;
	}

	private boolean searchKeywordArticle(Article a, String keyword) {
		List<Article> ll = byKeyword.get(keyword);
		if (ll != null) {
			Iterator<Article> it = ll.iterator();
			while (it.hasNext()) {
				if (it.next() == a) {
					return true;
				}
			}
		}
		return false;
	}
	
	private boolean searchArticleAuthor(String author) {
		List<Article> ll = byAuthor.get(author);
		if (ll != null) {
			Iterator<Article> it = ll.iterator();
			while (it.hasNext()) {
				Article a = it.next();
				if (a != byArticleId.get(a.getId()))
					return false;
			}
		}
		else
			return false;
		return true;
	}

	private boolean searchArticleKeyword(String keyword) {
		List<Article> ll = byKeyword.get(keyword);
		if (ll != null) {
			Iterator<Article> it = ll.iterator();
			while (it.hasNext()) {
				Article a = it.next();
				if (a != byArticleId.get(a.getId()))
					return false;
			}
		}
		else
			return false;
		return true;
	}

}
