public class Test {
	
	public static void main(String args[]) {
		Node root = new Node(
				20,
				new Node(10,new Node(13,null,null),new Node(8,null,new Node(5,null,null))),
				new Node(13,
						new Node(4,null,new Node(1,null,null)),
						new Node(2,null,null)));
	
		String line = "ANTH2012-01	0.29934210526315796";
		String keyString = "", valueString="";
		int tabSpaceIndex = line.indexOf('\t');
			keyString = line.substring(0,tabSpaceIndex);
			valueString = line.substring(tabSpaceIndex+1);
		System.out.println(keyString);
		System.out.println(valueString);
		
//		findPaths(root, 38, "", 0);
	}

	private static void findPaths(Node root, int x, String s, int sum) {
		if (root != null) {
			sum += root.data;
			s += String.valueOf(root.data) + " -> ";
			
			if (sum == x)
				System.out.println(s);
			
			/* Move left */
			findPaths(root.left, x, new String(s), sum);
			/* Move right */
			findPaths(root.right, x, new String(s), sum);
		}
	}
}

class Node {
	int data;
	public Node(int data, Node left, Node right) {
		super();
		this.data = data;
		this.left = left;
		this.right = right;
	}
	Node left;
	Node right;
}