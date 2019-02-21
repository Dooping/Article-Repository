package cp.articlerep.ds;

/**
 * @author Ricardo Dias
 */
public class LinkedList<V> implements List<V> {

	public class Node {
		final private V m_value;
		private Node m_next;

		public Node(V value, Node next) {
			m_value = value;
			m_next = next;
		}

		public Node(V value) {
			this(value, null);
		}

		public V getValue() {
			return m_value;
		}

		public void setNext(Node next) {
			m_next = next;
		}

		public Node getNext() {
			return m_next;
		}
	}

	private Node m_head;

	public LinkedList() {
		m_head = null;
	}

	public void add(V value) {
		m_head = new Node(value, m_head);
	}
	
	public void add(int pos, V value) {
		
		if (pos == 0) {
			add(value);
			return;
		}
		
		Node n = null;
		Node f = null;
		
		for (n=m_head; n != null && pos > 0; n=n.m_next) {
			f = n;
			pos--;
		}
		
		Node newNode = new Node(value, f.m_next);
		f.m_next = newNode;
	}

	public V remove(int pos) {
		V res = null;
		
		Node f = null;
		Node n = null;
		
		for (n=m_head; n != null && pos > 0; n=n.m_next) {
			f = n;
			pos--;
		}
		
		if (n != null) {
			res = n.m_value;
			if (f != null) {
			    f.m_next = n.m_next;
			}
			else {
			    m_head = n.m_next;
			}
		}

		return res;
	}

	public V get(int pos) {
		V res = null;
		Node n = null;
		for (n=m_head; n != null && pos > 0; n=n.m_next) {
			pos--;
		}
		if (n != null) {
			res = n.m_value;
		}
		return res;
	}

	public int size() {
		int res=0;
		for (Node n=m_head; n != null; n=n.m_next) {
			res++;
		}
		return res;
	}

	public Iterator<V> iterator() {
		return new Iterator<V>() {
			
			private Node curr = m_head;
			
			public boolean hasNext() {
				return curr != null;
			}
			public V next() {
				V ret = curr.m_value;
				curr = curr.m_next;
				return ret;
			}
		};
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer("[");
		
		Iterator<V> it = this.iterator();
		
		if (it.hasNext()) {
			sb.append(it.next());
		}
		
		while(it.hasNext()) {
			sb.append(", "+it.next());
		}
		
		sb.append("]");
		
		return sb.toString();
	}
}
