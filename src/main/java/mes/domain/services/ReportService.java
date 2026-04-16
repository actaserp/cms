package mes.domain.services;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import mes.config.Settings;
import mes.domain.model.ReportFileResult;
import net.sf.jasperreports.engine.JRPrintPage;
import net.sf.jasperreports.engine.JasperCompileManager;
import net.sf.jasperreports.engine.JasperExportManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.JasperReport;
import java.util.UUID;

@Service
public class ReportService {
	
	@Autowired
	DataSource dataSource;	
	
	@Autowired
	Settings settings;
	
	/**
	 * 
	 * @param jrxmlpath
	 * @param param
	 * @return
	 */
	public ReportFileResult create(String jrxmlFilename, Map<String, Object> param) {
		ReportFileResult result = new ReportFileResult();
		result.jrxmlFilename = jrxmlFilename;
		result.param = param;
		UUID one = UUID.randomUUID();
		
		String basePath = settings.getProperty("file_upload_path");
		String pdfPath = basePath + "jasper_report\\";
		String jasperPath = basePath + "jasper_files\\"+jrxmlFilename;
		
		String pdfFilename = String.format("%s.pdf", one.toString());
		String pdfFilePath =  String.format("%sjasper_report%s",pdfPath, pdfFilename);
		
		result.pdfFilename = pdfFilename;
		result.pdfPath = pdfFilePath;
		
		try {
			JasperPrint jasperPrint = this.createJasper(jasperPath, param);
			JasperExportManager.exportReportToPdfFile(jasperPrint, pdfFilePath);
			result.success = true;
		} catch (Exception e) {
			result.success = false;
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 
	 * @param paramList
	 * @return
	 */
	public ReportFileResult createMerge(List<Map<String, Object>> paramList ) {
		ReportFileResult result = new ReportFileResult();
		
		String basePath = settings.getProperty("file_upload_path");
		String pdfPath = basePath + "jasper_report\\";
		
		UUID one = UUID.randomUUID();
		String pdfFilename = String.format("%s.pdf", one.toString());
		String pdfFilePath =  String.format("%sjasper_report%s",pdfPath, pdfFilename);
		
		result.pdfFilename = pdfFilename;
		result.pdfPath = pdfFilePath;
		
		try {
	        JasperPrint mainJasperPrint = null;
	        for (Map<String, Object> paramItem : paramList) {
	            String jrXmlPath = String.valueOf(paramItem.get("jrxmlpath"));
	            String jasperPath = basePath + "jasper_files\\"+jrXmlPath;
	            
	            
	            if (mainJasperPrint == null) {
	                mainJasperPrint = this.createJasper(jasperPath, paramItem);
	            } else {
	                JasperPrint jasperPrint = this.createJasper(jasperPath, paramItem);
	                for (JRPrintPage jrPrintPage : jasperPrint.getPages()) {
	                    mainJasperPrint.addPage(jrPrintPage);
	                }
	            }
	        }
	        
	        result.success = true;
	        
	        JasperExportManager.exportReportToPdfFile(mainJasperPrint, pdfPath);
		
		} catch (Exception e) {
			result.success = false;
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 
	 * @param jrXmlPath
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
    private JasperPrint createJasper(String jrXmlPath, Map<String, Object> parameters) throws Exception {
        JasperReport jasperReport = JasperCompileManager.compileReport(jrXmlPath);
        JasperPrint jasperPrint = JasperFillManager.fillReport(jasperReport, parameters, this.dataSource.getConnection());
        return jasperPrint;
    }
}