
public class Version {
	private int value = -1;
	private int timestamp = -1;
	
	//This indicates which transaction wrote to this version
	//-1 transactionNumbe indicates it was the DataManager.load() that loaded it
	private int fromTransactionNumber = -1;
	
	public Version(int value, int timestamp, int transactionNumber){
		this.value = value;
		this.timestamp = timestamp;
		this.setFromTransactionNumber(transactionNumber);
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
	public int getFromTransactionNumber() {
		return fromTransactionNumber;
	}
	public void setFromTransactionNumber(int fromTransactionNumber) {
		this.fromTransactionNumber = fromTransactionNumber;
	}
	
}
