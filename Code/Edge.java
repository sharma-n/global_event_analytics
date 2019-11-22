import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Edge implements WritableComparable<Edge> {
	private Text firstNode;
	private Text secondNode;
	
	public Edge() {
		this.firstNode = new Text();
		this.secondNode = new Text();
	}
	
	public Edge(String first, String second){
		firstNode.set(first);
		secondNode.set(second);
	}
	
	public Edge(Text first, Text second){
		firstNode = first;
		secondNode = second;
	}
	
	public void set(Text first, Text second){
		firstNode = first;
		secondNode = second;
	}
	
	public Text getFirst() {return firstNode;}
	
	public Text getSecond() {return secondNode;}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		firstNode.readFields(in);
		secondNode.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		firstNode.write(out);
		secondNode.write(out);
	}

	@Override
	public int compareTo(Edge o) {
		return (firstNode.compareTo(o.firstNode) != 0)
				? firstNode.compareTo(o.firstNode)
				: secondNode.compareTo(o.secondNode);
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof Edge)
			return (firstNode.equals(((Edge) o).firstNode) && secondNode.equals(((Edge) o).secondNode));
		return false;
	}
	
	@Override
	public int hashCode() {
		return firstNode.hashCode() + secondNode.hashCode();
	}
	
	public String toString(){
		return firstNode.toString() + "," + secondNode.toString();
	}
}