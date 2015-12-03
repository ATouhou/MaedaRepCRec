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
			ROTransaction roT = new ROTransaction(this.dataManager, transactionNumber, currentTimestamp );
			this.activeTransactions.put(transactionNumber, roT);
			
		}else if(command[0].equals("begin")){
			
			// command = ["begin", transactionNum]
			//Create a Transaction with threads for each site; Add Transaction to activeTransactions. 
			int transactionNumber = Integer.parseInt(command[1]);
			RWTransaction rwT = new RWTransaction(this.dataManager, transactionNumber, currentTimestamp);
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
				if(!currentTransaction.isHasWriteLock(siteIndex, variableIndex)){
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
			//If the transaction is a read-write transaction,
			//	acquires a lock as in 2pl and read from one copy (available copies algorithm)

			int transactionNumber = Integer.parseInt(command[1]);
			Transaction currentTransaction = this.activeTransactions.get(transactionNumber);

			if(currentTransaction.getReadOnly()){
				ROTransaction roTransaction = (ROTransaction) this.activeTransactions.get(transactionNumber);
				roTransaction.processOperation("R", command, currentTimestamp);
				
			}else{
				RWTransaction rwTransaction = (RWTransaction) this.activeTransactions.get(transactionNumber);
				
				//Check data manager to see which sites the copies are.
				//And which locks are already acquired
				//If there is no locks for one site, then acquire the lock
				
				int variableIndex = Integer.parseInt(command[2]);
				List<Integer> siteIndexesToWrite  = this.dataManager.getAvailableSitesVariablesWhere(variableIndex);
				
				boolean isRead = false;
				for(Integer siteIndex: siteIndexesToWrite){
					if(rwTransaction.isHasReadLock(siteIndex, variableIndex)){
						//Once a read lock has been found, then read it, and then stop.
						
						//Add the index site to the command details
						String[] modifyCommand = new String[]{command[0], command[1], command[2], siteIndex+""};
						rwTransaction.processOperation("R", modifyCommand, currentTimestamp);
						isRead = true;
						break;
					}
				}
				
				
				if(!isRead){
					//If no read is done, because there aren't any locks, acquire a lock on any one site
					boolean isLockAcquired = false;
					for(Integer siteIndex: siteIndexesToWrite){
						Lock lock = this.dataManager.getSite(siteIndex).requestLock(transactionNumber, variableIndex, false);
						if(lock!=null){
							
							String[] modifyCommand = new String[]{command[0], command[1], command[2], siteIndex+""};
							rwTransaction.processOperation("R", modifyCommand, currentTimestamp);
							isLockAcquired = true;
							break;
							
						}
					}
					
					//If the lock was not acquired, then either wait or die, based on the timestamps of the 
					//conflicting transactions
					if(!isLockAcquired){
						//TODO: wait-die protocol
						this.queuedOperations.add(command);
						return;
					}
					
				}
				
			}
			
		}else if(command[0].equals("end")){
			//command = ["end", transaction number]
			
			int transactionNumber = Integer.parseInt(command[1]);
			Transaction currentTransaction = this.activeTransactions.get(transactionNumber);

			//Report whether the transaction can commit
			//If the transaction was a read write transaction: validate 
			if(!currentTransaction.getReadOnly()){
				
				//If valid, commit all transactions and release the transaction’s locks i.e. Transaction.releaseLocks() 
				//Otherwise, abort(current transaction)
				RWTransaction rwTransaction = (RWTransaction) this.activeTransactions.get(transactionNumber);
				
				if(validate(transactionNumber)){
					rwTransaction.commit();
				}else{
					//If it is not valid, abort it
					rwTransaction.abort();
				}
				
				//Once the transaction commits/aborts, it can release the locks
				rwTransaction.releaseLocks();
			}else{
				//Otherwise, if the transaction was read only, then say yes
				System.out.println("T"+transactionNumber+" can commit");
				ROTransaction roTransaction = (ROTransaction) this.activeTransactions.get(transactionNumber);
				roTransaction.commit();
			}
			
		}
		/*
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
	/*private void abort(Transaction t){
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
		List<Integer> sitesAccessed = transaction.getSiteIndexesAccessed();
		for(int key: sitesAccessed){
			//If one of the sites has failed, then return false
			this.dataManager.getSite(key).isSiteDownAfter(transaction.getBeginningTimestamp());
			
		}
		return true;
	}
}
