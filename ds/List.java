package cp.articlerep.ds;

/**
 * @author Ricardo Dias
 */
public interface List<V> {
	
	public void add(V value);
	public void add(int pos, V value);
	public V remove(int pos);
	public V get(int pos);
	public int size();
	
	Iterator<V> iterator();
	
}
