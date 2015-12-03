import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataManager {
	//Integer refers to the site index
	private Map<Integer, Site> allSites = new HashMap<Integer, Site>();
	
	/*
	 * Loads the sites and their variables, including the variable's initial values.
	 */
	public void loadDatabase(){
		//20 Variables
		//10 sites
		//Create 10 sites
		for(int i=1; i<=10; i++){
			allSites.put(i, new Site(i));
		}
				
		//load even variables into the right one
		//Even indexed variables are at all sites.
		for(Integer key: allSites.keySet()){
			Site site = allSites.get(key);
			for(int indexVariable=2; indexVariable<=20; indexVariable = indexVariable+2){
				int value = 10 * indexVariable;
				Variable evenVariable = new Variable(indexVariable, value, 0);
				site.addVariableAtSite(indexVariable, evenVariable);
			}
		}
				
		//Load odd variables into the appropriate site
		//The odd indexed variables are at one site each (i.e. 1 + index number mod 10 )
		for(int indexVariable=1; indexVariable<=20; indexVariable = indexVariable+2){
			int value = 10 * indexVariable;
			Variable oddVariable = new Variable(indexVariable, value, 0);
			
			//Store the variable at siteIndex = 1 + index number mod 10
			int siteIndex = 1 + (indexVariable % 10);
			allSites.get(siteIndex).addVariableAtSite(indexVariable, oddVariable);
			
		}
	}
	
	/*
	 * @return the site indexes where @variableIndex is in and where the sites aren't down
	 * i.e. @variableIndex = 2, then [1, 2, ..., 20] since it is at all sites
	 */
	public List<Integer> getAvailableSitesVariablesWhere(int variableIndex){
		List<Integer> siteIndexes = new ArrayList<Integer>();
		for(Integer key: allSites.keySet()){
			Site site = allSites.get(key);
			if(site.isSiteContainVariableIndex(variableIndex) && !site.isSiteDown()){
				siteIndexes.add(key);
			}
		}
		return siteIndexes;
	}
	
	/*
	 * @return the site given the @siteIndex
	 */
	public Site getSite(int siteIndex){
		return this.allSites.get(siteIndex);
	}
}
