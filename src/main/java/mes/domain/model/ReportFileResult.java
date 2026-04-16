package mes.domain.model;

import java.util.Map;

public class ReportFileResult{
	
	public String jrxmlFilename;
	public String pdfFilename;
	public String pdfPath;	
	public Integer attach_id;
	
	public boolean success;	
	public String message;	
	public Map<String, Object> param;
}
