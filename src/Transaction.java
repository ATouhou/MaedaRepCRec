import java.util.HashMap;
import java.util.List;
import java.util.Map;

//This is the basic template for every type of transaction
public interface Transaction {
	/*
		A transaction can be read only or read-write. 
		If it is an ROTransaction, then getReadOnly() = true.
		If it is an RWTransaction, then getReadOnly() = false.
	 */
	void setReadOnly(boolean input);
	boolean getReadOnly();
	
	int getTransactionNumber();
	
	//Integer refers to the site index
	Map<Integer, Site> siteAccessed = new HashMap<Integer, Site>();
	
	void processOperation(String operation, String[] inputs, int currentTimestamp);
	
	void releaseLocks(int currentTimestamp);
	
	void commit(int currentTimestamp);
	
	void abort(int currentTimestamp);
	
	List<Integer> getSiteIndexesAccessed();
	
	//This returns the timstamp at which the transaction began
	int getBeginningTimestamp();
	
	//Operations queued from this Transaction
	List<String[]> getQueuedOperations();
	void addQueuedOperation(String[] queuedOperations);
	void removeQueuedOperation(String[] queuedOperations);
	
	//Information on whether this transaction is waiting
	boolean isWaiting();
	
	//Get the transaction number of the transaction holding this transaction back
	//i.e. if transaction 1 waits for transaction 2, then transaction 1's getTransactionWaitFor() = 2
	int getTransactionWaitFor();
	
	//@transactionNumWait is the number that is causing this transaction to wait
	void setToWaitFor(int transactionNumWait);
	
	//Set the transaction to active
	void setTransactionToActive();

}
