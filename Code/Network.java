import java.util.HashMap;
import java.util.Map.Entry;

class Link<T>{
	private T firstNode;
	private T secondNode;
	
	public Link(T first, T second) {
		firstNode = first;
		secondNode = second;
	}

	public T getFirst() {return firstNode;}

	public T getSecond() {return secondNode;}

	public String print() {return (firstNode+", "+secondNode);}

	@Override
	public int hashCode() {
		final int prime = 23;
		int result = 1;
		result = prime * (result + ((firstNode == null) ? 0 : firstNode.hashCode()) + ((secondNode==null) ? 0 : secondNode.hashCode()));
		return result;
	}

	@Override
	public boolean equals(Object object){
		if(!(object instanceof Link)){
			if(!((Link<T>) object).getFirst().equals(firstNode))
				return false;
			else if(!((Link<T>) object).getSecond().equals(secondNode))
				return false;
			else
				return true;
		}
		else
			return false;
	}
}

class Network {
	private static final String [][] array = {{"1", "5", "3", "7", "14", "8"},
		{"5", "6", "2", "7", "12", "9"},{"2", "4", "6", "8", "10", "12"},{"1", "3", "5", "7", "9", "11"}};

	public static void main(String[] args) {
		HashMap<Link<String>,Integer> network = new HashMap<Link<String>,Integer>();
		for(String [] nums : array){
			for(int i=0; i<nums.length-1; i++){
				for(int j=i+1; j<nums.length; j++){
					Link<String> temp;
					if(nums[i].compareTo(nums[j])<0)
						temp = new Link<String>(nums[i], nums[j]);
					else
						temp = new Link<String>(nums[j], nums[i]);
					if(network.containsKey(temp))
						network.put(temp, network.get(temp)+1);
					else
						network.put(temp, 1);
				}
			}
		}
		for(Entry<Link<String>, Integer> elem : network.entrySet()){
			System.out.println(elem.getKey().print() + ": " + elem.getValue());
		}
	}
}