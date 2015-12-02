
public class Version {
	private int value = -1;
	private int timestamp = -1;
	
	public Version(int value, int timestamp){
		this.value = value;
		this.timestamp = timestamp;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	public int getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}
	
}
