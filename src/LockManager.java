import java.util.ArrayList;
import java.util.List;

public class LockManager {
	List<Transaction> liveTransactions = new ArrayList<Transaction>();
	//TODO:
	//Map<> lockTable = new Arra
	
	public void releaseSiteLock(){
		this.liveTransactions = new ArrayList<Transaction>();
	}
	
	/*
	 * TODO:During shut down, the table is erased
	 */
	public void eraseTable(){
		//lockTable = new Ar;
	}
	 
}
