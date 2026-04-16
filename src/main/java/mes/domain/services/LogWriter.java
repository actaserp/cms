package mes.domain.services;
public interface LogWriter {
	public void addDbLog(String logType, String source, Exception ex);
	public String findDeleteErrorMessage(Exception ex);
	public String findForeignTable(String message) ;	
	public String findForeignTableInIntegrity(String message);
}
