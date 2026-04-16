package mes.app.precedence;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.Transactional;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.precedence.service.EquipHistoryService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.EquipmentHistory;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.EquipmentHistoryRepository;
import mes.domain.repository.RelationDataRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/equip_history")
public class EquipHistoryController {
	
	@Autowired
	private EquipHistoryService equipHistoryService;
	
	@Autowired
	EquipmentHistoryRepository equipmentHistoryRepository;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	RelationDataRepository relationDataRepository;
	
	@Autowired
	SqlRunner sqlRunner;

	// 결재현황 조회
	@GetMapping("/appr_stat")
	public AjaxResult getEquipHistoryApprStatus(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="appr_state", required=false) String apprState,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.equipHistoryService.getEquipHistoryApprStatus(startDate,endDate,apprState);     
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 설비변동사항 조회
	@GetMapping("/read")
	public AjaxResult getEquipHistoryList(
    		@RequestParam(value="bh_id", required=false) Integer bh_id, 
			HttpServletRequest request) {
		
        Map<String, Object> items = this.equipHistoryService.getEquipHistoryList(bh_id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
		
	// 설비변동사항 상세조회
	@GetMapping("/detail")
	public AjaxResult getEquipHistoryDetailList(
    		@RequestParam(value="id", required=false) Integer id,
			HttpServletRequest request) {
			
		Map<String, Object> items = this.equipHistoryService.getEquipHistoryDetailList(id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 설비변동사항 저장
	@PostMapping("/save")
	public AjaxResult saveEquipHistory(
	        @RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bh_id,
	        @RequestParam(value="title", required=false) String title,
	        @RequestParam(value="data_date", required=false) String data_date,
	        @RequestParam(value="equ_history_id", required=false) Integer equ_history_id,
	        @RequestParam(value="equ_id", required=false) Integer equ_id,
	        @RequestParam(value="cost", required=false) Integer cost,
	        @RequestParam(value="manager", required=false) String manager,
	        @RequestParam(value="part_leader", required=false) String part_leader,
	        @RequestParam(value="content", required=false) String content,
	        @RequestParam(value="description", required=false) String description,
	        HttpServletRequest request,
	        Authentication auth) {

	    User user = (User)auth.getPrincipal();

	    AjaxResult result = new AjaxResult();

	    BundleHead bh = null;

	    if (bh_id > 0) {
	    	bh = this.bundleHeadRepository.getBundleHeadById(bh_id);
	    } else {
	    	// 최초 등록
	    	bh = new BundleHead();
	        bh.setTableName("equip_history");
	        bh.setChar1(title);
	        bh.setDate1(CommonUtil.tryTimestamp(data_date));
	        bh.set_audit(user);
	        bh = this.bundleHeadRepository.save(bh);
	    }

	    EquipmentHistory equipmentHistory = null;

	    if (equ_history_id != null) {
	    	equipmentHistory = this.equipmentHistoryRepository.getEquipmentHistoryById(equ_history_id);
	    } else {
	    	// 최초 등록
	    	equipmentHistory = new EquipmentHistory();
	    }

	    equipmentHistory.setEquipmentId(equ_id);
	    equipmentHistory.setDataDate(Date.valueOf(data_date));
	    equipmentHistory.setContent(content);
	    equipmentHistory.setDescription(description);
	    equipmentHistory.setCost(cost);
	    equipmentHistory.setChar1(manager);
	    equipmentHistory.setChar2(part_leader);
	    equipmentHistory.setApprDataPk(bh.getId());
	    equipmentHistory.setApprTableName("bundle_head");
	    equipmentHistory.set_status("history");
	    equipmentHistory.set_audit(user);
	    equipmentHistory = this.equipmentHistoryRepository.save(equipmentHistory);

	    Map<String, Object> item = new HashMap<String, Object>();
	    item.put("id", bh.getId());
	    result.data = item;
	    return result;
	}

		
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteEquipHistory(
			@RequestParam(value="bh_id", required=false) Integer bh_id) {
		
		AjaxResult result = new AjaxResult();
		
		if (bh_id != null) {
			this.equipmentHistoryRepository.deleteByApprDataPk(bh_id);
			this.bundleHeadRepository.deleteById(bh_id);
		}
		
		return result;
		
	}
	
	@GetMapping("/excelDown")
	public void excelDownload(@RequestParam MultiValueMap<String,Object> param,
								HttpServletRequest request, 
								HttpServletResponse response) 
			throws IOException,UnsupportedEncodingException {
		

		Workbook wb = new XSSFWorkbook();
		Sheet sheet = wb.createSheet("sheet1");

		Row row = null;
		Cell cell = null;
		int rowNum = 0;

		// Header
		row = sheet.createRow(rowNum++);
		cell = row.createCell(0);
		cell.setCellValue("설비");
		cell = row.createCell(1);
		cell.setCellValue("금액(천원)");
		cell = row.createCell(2);
		cell.setCellValue("담당");
		cell = row.createCell(3);
		cell.setCellValue("파트장");
		cell = row.createCell(4);
		cell.setCellValue("주요내용");
		cell = row.createCell(5);
		cell.setCellValue("특이사항");
		
		Map<String, Object> items = CommonUtil.loadJsonToMap(param.getFirst("param").toString());
		// Body
		
		row = sheet.createRow(rowNum++);
		cell = row.createCell(0);
		cell.setCellValue(items.get("equ_id").toString());
		cell = row.createCell(1);
		cell.setCellValue(items.get("cost").toString());
		cell = row.createCell(2);
		cell.setCellValue(items.get("manager").toString());
		cell = row.createCell(3);
		cell.setCellValue(items.get("part_leader").toString());
		cell = row.createCell(4);
		cell.setCellValue(items.get("content").toString());
		cell = row.createCell(5);
		cell.setCellValue(items.get("description").toString());

		sheet.autoSizeColumn(0);
		sheet.autoSizeColumn(1);
		sheet.autoSizeColumn(2);
		sheet.autoSizeColumn(3);
		sheet.autoSizeColumn(4);
		sheet.autoSizeColumn(5);
	    
		// 컨텐츠 타입과 파일명 지정
		String fileName = "설비점검이력";
		
		response.setContentType("ms-vnd/excel;charset=UTF-8");
		response.setHeader("Content-Disposition", "attachment;filename="+ URLEncoder.encode(fileName, "UTF-8") +".xlsx");
	    
		wb.write(response.getOutputStream());
		wb.close();
	}
}
