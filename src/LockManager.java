import java.util.ArrayList;
import java.util.List;

/*
 * Each site has its own lock manager. The lock manager manages all the locks on its site's variables
 */
public class LockManager {
	List<Transaction> liveTransactions = new ArrayList<Transaction>();
	List<Lock> activeLocks = new ArrayList<Lock>();
	
	public void releaseSiteLock(){
		this.liveTransactions = new ArrayList<Transaction>();
	}
	
	
	 
}
