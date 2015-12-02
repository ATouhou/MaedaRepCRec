import java.util.ArrayList;
import java.util.List;

public class Variable {
	//An index, a value from 1 to 20, referring to one of the 20 distinct variables
	private int indexVariable = -1;
	
	private Version currentVersion = null;
	private Version lastCommittedVersion = null;
	//Don't need versions when everything is in the site's log
	//private List<Version> versions = new ArrayList<Version>();
	
	// isAllowRead true, by default, until fail()
	private boolean isAllowRead = true;
	
	// indicates whether there is a replication of itself at other sites
	private boolean isVariableReplicated; 
	
	//	isLock has access control by a semaphore i.e. synchronized, ensuring that only one write thread has access
	// TODO: locks must be kept seperate from variable because there can be upgrades from read to write
	private boolean isLock = false;
	
	public Variable(int indexVariable, int value, int timestamp){
		this.setIndexVariable(indexVariable);
		this.currentVersion = new Version(value, timestamp);

	}
	/*
	 * Update current version to value
	 * TODO: What is the timestamp of the final written value? The current
	 * TODO: timestamp when it was written or the transaction's timestamp?
	 */
	public void writeToCurrentVersion(int value, int timestamp){
		this.currentVersion = new Version(value, timestamp);
	}
	/*
	 * Set the currentVersion to the before image of t
	 */
	public void restoreBeforeImage(Transaction t){
		//TODO:Set the currentVersion to the before image of t
	}
	
	/*
	 * If isAllowRead = false, then set it to true, 
	 * now that there is a current new update to it Set lastCommittedVersion to the currentVersion
	 * Add a before image and timestamp to the list of versions
	 */
	public void commit(){
		//TODO: commit
	}
	
	public Version getCurrentVersion(){
		return this.currentVersion;
	}
	
	public String toString(){ 
		return currentVersion.getTimestamp()+" " +currentVersion.getValue() + "\n";
	}
	public boolean isAllowRead() {
		return isAllowRead;
	}
	public void setAllowRead(boolean isAllowRead) {
		this.isAllowRead = isAllowRead;
	}
	public int getIndexVariable() {
		return indexVariable;
	}
	public void setIndexVariable(int indexVariable) {
		this.indexVariable = indexVariable;
	}
}
