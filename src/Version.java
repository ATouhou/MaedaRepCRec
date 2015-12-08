
public class Version {
	private int value = -1;
	
	//Timestamp at which this version was created
	private int timestamp = -1;
	
	//This indicates which transaction wrote to this version
	//-1 transactionNumbe indicates it was the DataManager.load() that loaded it
	private int fromTransactionNumber = -1;
	
	//Is this version committed?
	private boolean isCommitted = false;
	
	public Version(int value, int timestamp, int transactionNumber, boolean isCommitted){
		this.value = value;
		this.timestamp = timestamp;
		this.setFromTransactionNumber(transactionNumber);
		this.isCommitted = isCommitted;
	}
	public int getValue() {
		return value;
	}
	public int getTimestamp() {
		return timestamp;
	}
	public int getFromTransactionNumber() {
		return fromTransactionNumber;
	}
	public void setFromTransactionNumber(int fromTransactionNumber) {
		this.fromTransactionNumber = fromTransactionNumber;
	}
	
	public boolean isCommitted(){
		return this.isCommitted;
	}
	
	public void setCommitted(){
		this.isCommitted = true;
	}
	
	public String toString(){
		return "Version(value="+value+", timestamp="+timestamp+",isCommitted="+isCommitted+")";
	}
}
