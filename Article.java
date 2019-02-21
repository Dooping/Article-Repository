package cp.articlerep;

import cp.articlerep.ds.LinkedList;
import cp.articlerep.ds.List;
import cp.articlerep.ds.Iterator;
import java.util.concurrent.locks.*;

public class Article {
	
	private int id;
	private String name;
	private List<String> authors;
	private List<String> keywords;
	
	public Article(int id, String name) {
		this.id = id;
		this.name = name;
		this.authors = new LinkedList<String>();
		this.keywords = new LinkedList<String>();
	}
	
	public void addAuthor(String author) {
		this.authors.add(author);
	}
	
	public void addKeyword(String keyword) {
		this.keywords.add(keyword);
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getAuthors() {
		return authors;
	}

	public List<String> getKeywords() {
		return keywords;
	}

	public String toString() {
		String r = "(" + id + ", " + name;
		int i;

		r += ",[";
		i = 0;

		for (Iterator<String> it = authors.iterator(); it.hasNext();) {
			r += (i > 0 ? "," : "") + it.next();
			i++;
		}

		r += "],[";
		i = 0;

		for (Iterator<String> it = keywords.iterator(); it.hasNext();) {
			r += (i > 0 ? "," : "") + it.next();
			i++;
		}

		r += "])";

		return r;
	}
}
