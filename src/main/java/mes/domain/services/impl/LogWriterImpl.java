package mes.domain.services.impl;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import mes.domain.entity.SystemLog;
import mes.domain.repository.SystemLogRepository;
import mes.domain.services.LogWriter;

@Service
public class LogWriterImpl implements LogWriter {
	@Autowired
	SystemLogRepository systemLogRepository;
	
	public void addDbLog(String logType, String source, Exception ex) {

		String spjangcd = TenantContext.get();
		String message = null;
		if (ex !=null) {
			message = ex.toString();
		}
		
		SystemLog systemLog = new SystemLog();
		
		systemLog.setType(logType);
		systemLog.setSource(source);
		systemLog.setMessage(message);
		systemLog.setSpjangcd(spjangcd);
		
		this.systemLogRepository.save(systemLog);
	}
	
	public String findDeleteErrorMessage(Exception ex) {
		String message = null;
		
		
		
		return message;
	}
	
	/**
	 * 
	 * @param message
	 * @return
	 */
	public String findForeignTable(String message) {
		
		String tableName = null;
		
		
		return tableName;
		
	}
	
	/**
	 * 
	 * @param message
	 * @return
	 */
	public String findForeignTableInIntegrity(String message) {
		
		String tableName = null;
		
		
		return tableName;
		
	}
}
