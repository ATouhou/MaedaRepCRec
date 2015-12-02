import java.util.ArrayList;
import java.util.List;

public class Site {
	private List<Variable> siteVariables = new ArrayList<Variable>();
	private boolean isSiteDown;
	//Nothing shal pass the site without the log knowing of it
	private Log siteLog;
	
	//Site index
	private int siteIndex = -1;
	
	//Each site has lockmanager
	private LockManager lockmanager = new LockManager();
	
	public Site(int siteIndex){
		this.siteIndex = siteIndex;
		setSiteDown(false);
	}
	
	/*
	 * Set all variable's Site.isAllowRead = false
	 * All sites must release locks from living transactions that were also live at that site.
	 */
	public void fail(){
		setSiteDown(true);
		//Set all variable's Site.isAllowRead = false
		for(Variable variable: siteVariables){
			variable.setAllowRead(false);
		}
		lockmanager.releaseSiteLock();
	}
	/*
	 * @return String currentState for all Variables
	 */
	public String dump(){
		String currentState = "";
		for(Variable variable: siteVariables){
			currentState = currentState + variable.toString();
		}
		return currentState;
	}

	public List<Variable> getVariableSite() {
		return this.siteVariables;
	}

	public void addVariableAtSite(Variable variable) {
		this.siteVariables.add(variable);
	}
	
	public void setVariablesAtSite(List<Variable> variables){
		this.siteVariables = variables;
	}
	
	public Variable getCommittedVariable(int indexVariable){
		for(int i=0; i<this.siteVariables.size(); i++){
			if(this.siteVariables.get(i).getIndexVariable() == indexVariable ){
				return this.siteVariables.get(i);
			}
		}
		return null;
	}
	
	public boolean isSiteDown() {
		return isSiteDown;
	}
	
	/*
	 * Check whether the site went down after a particular timestamp. This is used for validating a transaction
	 */
	public boolean isSiteDownAfter(int timestamp) {
		int lastFailEvent =  this.siteLog.findLastFailTimestamp();
		
		//If the lastFailEvent happenned after the timestamp, then return true
		if(lastFailEvent>timestamp && lastFailEvent!=-1){
			return true;
		}
		return false;
	}
	
	public void setSiteDown(boolean isSiteDown) {
		this.isSiteDown = isSiteDown;
	}

	public int getSiteIndex() {
		return siteIndex;
	}

}
