import java.util.HashMap;
import java.util.Map;

public interface Transaction {
	/*
		A transaction can be read only or read and write
	 */
	
	void setReadOnly(boolean input);
	boolean getReadOnly();
	
	//Integer refers to the site index
	Map<Integer, Site> siteAccessed = new HashMap<Integer, Site>();
	
	void processOperation(String operation, String[] inputs);
	
	void releaseLocks();
	
	void commit();
	
	Map<Integer, Site> getSitesAccessed();
	
	//This returns the timstamp at which the transaction began
	int getBeginningTimestamp();
}
