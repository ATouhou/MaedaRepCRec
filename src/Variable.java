import java.util.ArrayList;
import java.util.List;

public class Variable {
	private boolean debug = false;
	
	//An index, a value from 1 to 20, referring to one of the 20 distinct variables
	private int indexVariable = -1;
	
	//current version is the latest version, but the last committed version the latest committed version
	private int latestVersion = -1;
	private int lastCommittedVersion = -1;
	
	// isAllowRead true, by default, until fail()
	private boolean isAllowRead = true;
	
	//List of all version of this variable
	private List<Version> allVersions = new ArrayList<Version>();
	
	public Variable(int indexVariable, int value, int timestamp, boolean isCommitted){
		this.setIndexVariable(indexVariable);
		this.allVersions.add(new Version(value, timestamp, 0, isCommitted));
		this.latestVersion = this.allVersions.size()-1;

		if(isCommitted){
			lastCommittedVersion = this.latestVersion;
		}
	}
	/******************************************************************************************************
	 * Read and writes and commit methods. The methods assume the Managers have already ensured the locks are there
	 *  and protocols are followed. So no locks are used. These methods are called only by Site.
	 ******************************************************************************************************/
	/*
	 * Read the version of this variable where the version's timestamp
	 * is less than @timestampBefore i.e. read the COMMITTED version before @timestampBefore
	 * This method is used by the multiversion protocol.
	 * @return is the value read
	 */
	public int readCommitted(int timestampBefore){
		if(this.isAllowRead){
			//Search the versions backwards
			for(int i=this.allVersions.size()-1; i>=0; i--){
				if(debug) System.out.println("Variable: Read is trying to find last committed version "+allVersions.get(i).toString());
				
				if(allVersions.get(i).isCommitted() && allVersions.get(i).getTimestamp()<timestampBefore){
					return this.allVersions.get(i).getValue();
				}
			}
		}
		return -1;
	}
	/*
	 * @return the latest version, which may not necessarily be committed
	 */
	public int readLatest(){
		if(this.isAllowRead){
			return this.allVersions.get(this.lastCommittedVersion).getValue();
		}
		return -1;
	}
	/*
	 * When a currently active transaction writes to a variable, it create a new version
	 * @value is the new value
	 * @timestamp is the current timestamp
	 * @transactionNumber is the current transaction doing this operation
	 */
	public void write(int value, int timestamp, int transactionNumber){
		boolean isCommitted = false;
		this.allVersions.add(new Version(value, timestamp, transactionNumber, isCommitted));
		this.latestVersion = this.allVersions.size()-1;

	}
	/*
	 * For recovery purposes, if Variable.isAllowRead = false, then set it to true, 
	 * now that there is a current new update to it Set lastCommittedVersion to the currentVersion
	 * Add a before image and timestamp to the list of versions
	 * @committingTransaction indicates which transaction is committing
	 * @return the committed value
	 */
	public int commit(int committingTransaction){
		//This means setting the versions written by the committing transaction and set that 
		//as the current version
		if(debug) System.out.println("Variable: Committing T"+committingTransaction);
		//Get the version with the latest timestamp of @committingTransaction from the list of versions
		int maxTimestamp = -1;
		int newCurrVersionIndex = -1; 
		int fromTransactionNumber = -1;
		boolean isValuesCommittedAfterLoad = false;
		for(int i=0; i<this.allVersions.size(); i++){
			Version version = this.allVersions.get(i);
			if(debug) System.out.println("Variable: "+version.toString());
			//Set the boolean value as appropriate
			if(version.getTimestamp()>maxTimestamp){
				isValuesCommittedAfterLoad = true;
			}
			//A version with the transaction number of 0 means that it was loaded by the database
			if((version.getFromTransactionNumber()==committingTransaction && version.getTimestamp()>maxTimestamp)
					){
				fromTransactionNumber = version.getFromTransactionNumber();
				newCurrVersionIndex = i;
				maxTimestamp = version.getTimestamp();
				if(debug) System.out.println("Variable: New max timestamp = "+maxTimestamp);
			}else if(version.getFromTransactionNumber()==0){
				fromTransactionNumber = version.getFromTransactionNumber();
				newCurrVersionIndex = i;
				maxTimestamp = version.getTimestamp();
				if(debug) System.out.println("Variable: New max timestamp from initial= "+maxTimestamp);
			}
		}
		//If fromTransactionNumber is 0 i.e. loaded from database while there are greater commits than that, then do not commit anything, since
		//we do not want to commit an initial value while other transactions have written to it
		if(fromTransactionNumber==0 && isValuesCommittedAfterLoad){
			if(debug) System.out.println("Variable: Dont commit anything by T"+committingTransaction+" since previous transactions have latest values.");
			
		}else{
			this.lastCommittedVersion = newCurrVersionIndex;
			
			if(debug) System.out.println("Variable: Last committed version located at "+lastCommittedVersion+" All versions="+toStringAllVersions());
	
			if(debug) System.out.println("Variable: T"+committingTransaction+" is committing "+this.allVersions.get(lastCommittedVersion).toString());
	
			this.allVersions.get(lastCommittedVersion).setCommitted();
			if(debug) System.out.println("Variable: T"+committingTransaction+" is done committing "+this.allVersions.get(lastCommittedVersion).toString());
			return this.allVersions.get(lastCommittedVersion).getValue();

		}
		return -1;
	}
	
	/******************************************************************************************************
	 * Recovery methods
	 ******************************************************************************************************/
	/*
	 * If this variable is replicated at other sites, then reset the pointers to the versions on this variable,
	 * setting them unreadable until a transaction writes to it.
	 */
	public void resetVersionIndexes(){
		this.lastCommittedVersion = -1;
		this.latestVersion = -1;
	}
	/******************************************************************************************************
	 * Setter, getter methods
	 ******************************************************************************************************/
	/*
	 * toString function for Variable
	 */
	public String toString(){ 
		return "Latest version:"+allVersions.get(this.latestVersion).getTimestamp()+" " +allVersions.get(this.latestVersion).getValue() + "\n"+
				"Latest committed version:"+allVersions.get(this.lastCommittedVersion).getTimestamp()+" "+allVersions.get(this.lastCommittedVersion).getValue()+"\n";
	}
	public String toStringLatestCommitted(){
		if(this.lastCommittedVersion!=-1)
			return ""+allVersions.get(this.lastCommittedVersion).getValue();// + " timestamp="+allVersions.get(this.lastCommittedVersion).getTimestamp();
		return "NA";
	}
	/*
	 * Setter and getters for @isAllowRead
	 */
	public boolean isAllowRead() {
		return isAllowRead;
	}
	public void setAllowRead(boolean isAllowRead) {
		this.isAllowRead = isAllowRead;
	}
	/*
	 * Setters and getters for @indexVariable
	 */
	public int getIndexVariable() {
		return indexVariable;
	}
	public void setIndexVariable(int indexVariable) {
		this.indexVariable = indexVariable;
	}
	private String toStringAllVersions(){
		String str = "[";
		for(Version v: this.allVersions){
			str = str + " "+v.toString();
		}
		str=str+"]";
		return str;
	}
	
	
}
