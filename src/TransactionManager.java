import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionManager {
	
	//Data Manager manages all the sites and knows where all the variables are
	DataManager dataManager = new DataManager();
	
	//Integer refers to the transaction number
	Map<Integer, Transaction> activeTransactions = new HashMap<Integer, Transaction>();
	
	//A list of commands, where command has the same format as the output of Parser.parseNextInstruction()
	List<String[]> queuedOperations = new ArrayList<String[]>();
		
	public TransactionManager(){
		//Load the database into the structure
		dataManager.loadDatabase();
	}
	
	
	/*
	 * 	@command is the array of details about the incoming command; it is the output of Parser.parse
	 * 	@currentTimestamp is the currentTimestamp.
	 */
	public void processOperation( String[] command, int currentTimestamp){
		//TODO:Check conflict with the lock table at each site
		//checkIsConflict();
		//If there aren't any deadlocks continue 
		
		if(command[0].equals("beginRO")){
			
			// command = ["beginRO", transactionNum]
			//If command = beginRO,
			//Create a Transaction and a thread; add Transaction to activeTransactions.
			int transactionNumber = Integer.parseInt(command[1]);
			ROTransaction roT = new ROTransaction(this.dataManager);
			this.activeTransactions.put(transactionNumber, roT);
			
		}else if(command[0].equals("begin")){
			
			// command = ["begin", transactionNum]
			//Create a Transaction with threads for each site; Add Transaction to activeTransactions. 
			int transactionNumber = Integer.parseInt(command[1]);
			RWTransaction rwT = new RWTransaction(this.dataManager, transactionNumber);
			this.activeTransactions.put(transactionNumber, rwT);
			
		}else if(command[0].equals("W")){
			
			// command = ["W", transaction number, index variable, value to write]
			//If command = write,
			//Send the operation to the appropriateTransaction
			int transactionNumber = Integer.parseInt(command[1]);

			//Check if the locks are already acquired
			//If the lock is acquired successfully, then the write operation happens successfully.
			//If the lock isn't acquired successfully, then either wait or die, depending on the 
			//timestamps of the conflicting transactions
			
			//Retrieve the current transaction 
			RWTransaction currentTransaction = (RWTransaction) this.activeTransactions.get(transactionNumber);
			
			//Check data manager to see which sites the copies are. 
			//Acquire locks and perform the write operation on ALL sites
			int variableIndex = Integer.parseInt(command[2]);
			List<Integer> siteIndexesToWrite  = this.dataManager.getAvailableSitesVariablesWhere(variableIndex);
			
			for(Integer siteIndex: siteIndexesToWrite){
				if(!currentTransaction.isHasLock(siteIndex, variableIndex)){
					//Acquire a write lock from the site's lock manager
					Lock lock = this.dataManager.getSite(siteIndex).requestLock(transactionNumber, variableIndex, false);
					
					//If the lock was not acquired, then either wait or die, based on the timestamps of the 
					//conflicting transactions
					if(lock==null){
						//TODO: wait-die protocol
						this.queuedOperations.add(command);
						return;
					}
					
				}
				
			}
			
			//At this point all needed locks are acquired, so write to a new version of the variables at ALL sites
			currentTransaction.processOperation("W", command, currentTimestamp);
			
		}else if(command[0].equals("R")){
			
			//command = [ "R", transaction number, index variable]
			
			//If the transaction is a read-only transaction, 
			// 	read a committed copy before the transaction began (multiversion protocol).
			//If the transaction is a read-write transaction, acquires a lock as in 2pl

			int transactionNumber = Integer.parseInt(command[1]);
			Transaction currentTransaction = this.activeTransactions.get(transactionNumber);

			if(currentTransaction.getReadOnly()){
				ROTransaction roTransaction = (ROTransaction) this.activeTransactions.get(transactionNumber);
				roTransaction.processOperation("R", command, currentTimestamp);

				
			}else{
				RWTransaction rwTransaction = (RWTransaction) this.activeTransactions.get(transactionNumber);

			}
			
		}
		/*
		
		If command = end,
		If the transaction was a read write transaction:
		validate(transactionNumber)
		If valid, commit all transactions and release the transaction’s locks i.e. Transaction.releaseLocks() Otherwise, abort(current transaction)
		If command = recover, Recover.recover(Site)
		If command = fail,
		Call Site.fail() for the site specified in the command
		If command = dump,
		for all Sites, Site.dump()
		*/
	}
	/*
	 * Wait-die protocol here
	 * @return Set<Transactions> conflicting transactions
	 */
	private List<Transaction> checkIsConflict(){ 
		//TODO: wait-die protocol here
		return null;
	}
	/*
	 * Restore before images of all sites accessed by t Release the locks held by t
	 */
	private void abort(Transaction t){
		//Restore before images of all sites accessed by t Release the locks held by t
	}
	/*
	 * If one of the sites accessed has failed after the transaction began, return false
	 * @return boolean
	 */
	private boolean validate(int transactionNumber){ 
		//If one of the sites accessed has failed after the transaction ran, return false
		Transaction transaction = this.activeTransactions.get(transactionNumber);
		
		//Loop through all the sites the transaction has accessed
		Map<Integer, Site> sitesAccessed = transaction.getSitesAccessed();
		for(int key: sitesAccessed.keySet()){
			//If one of the sites has failed, then return false
			sitesAccessed.get(key).isSiteDownAfter(transaction.getBeginningTimestamp());
			
		}
		return true;
	}
}
