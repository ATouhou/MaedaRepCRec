import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RWTransaction implements Transaction{
	private boolean isReadOnly = false;
	private DataManager dm = null; 
	private boolean isWait = false;
	
	//List of locks the transaction owns; this list reflects the lock tables on the sitess
	List<Lock> locks = new ArrayList<Lock>();
	
	private int transactionNumber = -1;
	
	//This is timestamp at which the transaction was created
	private int beginningTimestamp = -1;
	
	/*
	 * Instantiates the read-write transaction
	 */
	public RWTransaction(DataManager dm, int transactionNumber, int beginningTimestamp){
		this.dm = dm;
		this.transactionNumber = transactionNumber;
		this.beginningTimestamp = beginningTimestamp;
	}
	/*
	 *  Before calling this method, the TransactionManager has already checked for needed locks.
	 *  If the locks were not acquired properly, then this method is never called anyways. 
	 *  @operation is either "W" or "R"
	 * 	@command is the array of details about the incoming command; it is the output of Parser.parse
	 */
	@Override
	public void processOperation(String operation, String[] command, int currentTimestamp) {
		if(operation.equals("W")){
			
			//command = ["W", transaction number, index variable, value to write]
			int transactionNumber = Integer.parseInt(command[1]);
			int variableIndex = Integer.parseInt(command[2]);
			int valueToWrite = Integer.parseInt(command[3]);

			List<Integer> siteIndexesToWrite  = this.dm.getAvailableSitesVariablesWhere(variableIndex);
			
			for(Integer siteIndex: siteIndexesToWrite){
				dm.getSite(siteIndex).getVariable(variableIndex).write(valueToWrite, currentTimestamp, transactionNumber);
			}
			
			
		}
		
	}
	
	/*
	 * @return whether this transaction has the lock on  @variableIndex at @siteIndex
	 */
	public boolean isHasLock(int siteIndex, int variableIndex){
		for(Lock locks: this.locks){
			//Find a match for a lock based on @siteIndex and @variableIndex
			if(locks.getTransactionNumber()==this.transactionNumber 
					&& locks.getSiteIndex()==siteIndex
					&& locks.getLockedVariableIndex()==variableIndex){
				return true;
			}
		}
		return false;
	}

	@Override
	public void releaseLocks() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<Integer, Site> getSitesAccessed() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getBeginningTimestamp() {
		return this.beginningTimestamp;
	}

	@Override
	public void setReadOnly(boolean input) {
		this.isReadOnly = input;
	}

	@Override
	public boolean getReadOnly() {
		return this.isReadOnly;
	}

}
