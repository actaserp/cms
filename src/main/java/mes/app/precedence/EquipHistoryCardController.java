package mes.app.precedence;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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

import mes.app.common.service.FileService;
import mes.app.precedence.service.EquipHistoryCardService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.EquipmentHistory;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.EquipmentHistoryRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/precedence/equip_history_card")
public class EquipHistoryCardController {

	@Autowired
	private EquipHistoryCardService equipHistoryCardService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	EquipmentHistoryRepository equipmentHistoryRepository;
	
	@Autowired
	FileService fileService;
	
	@GetMapping("/appr_stat")
	public AjaxResult getApprStat(    		
			@RequestParam(value="start_date", required=false) String startDate, 
    		@RequestParam(value="end_date", required=false) String endDate, 
    		@RequestParam(value="appr_state", required=false) String apprState,
			HttpServletRequest request) {

		Map<String, Object> items = this.equipHistoryCardService.getApprStat(startDate,endDate,apprState);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@GetMapping("/read_in")
	public AjaxResult getReadIn(
			@RequestParam(value="bh_id", required=false) Integer bh_id,
			@RequestParam(value="equ_id", required=false) Integer equ_id, 
    		@RequestParam(value="from_date", required=false) String from_date,
    		@RequestParam(value="to_date", required=false) String to_date,
    		Authentication auth,
    		HttpServletRequest request) {
		User user = (User)auth.getPrincipal();
		
		Map<String, Object> items = this.equipHistoryCardService.getReadIn(bh_id,equ_id,from_date,to_date,user);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveEquipCard(
			@RequestParam(value="bh_id", required=false) Integer bh_id, 
    		@RequestParam(value="title", required=false) String title,
    		@RequestParam(value="equ_id", required=false) Integer equ_id, 
    		@RequestParam(value="from_date", required=false) String from_date,
    		@RequestParam(value="to_date", required=false) String to_date, 
    		@RequestParam(value="new_equ_id", required=false) String new_equ_id, 
    		@RequestParam MultiValueMap<String,Object> Q,
    		Authentication auth,
    		HttpServletRequest request) {
		
		User user = (User)auth.getPrincipal();
				
		AjaxResult result = new AjaxResult();
		
		BundleHead bh = new BundleHead();
		
		if(bh_id > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bh_id);
		}else {
			//신규의 경우에만 신규설비아이디 맵핑
			bh.setChar4(new_equ_id);
		}
		
		bh.setTableName("equip_history_card");
		bh.setChar1(title);
		bh.setChar2(from_date);
		bh.setChar3(to_date);
		bh.setDate1(CommonUtil.tryTimestamp(to_date));
		bh.setNumber1(CommonUtil.tryFloatNull(equ_id));
		bh.set_audit(user);
		bh = this.bundleHeadRepository.save(bh);
		
		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.size() > 0) {
			for(int i = 0; i < qItems.size(); i++) {
				Integer equ_history_id = CommonUtil.tryInt(CommonUtil.tryString(qItems.get(i).get("id")));
		    	
				if (equ_history_id != null) {
					EquipmentHistory eq_his = this.equipmentHistoryRepository.getEquipmentHistoryById(equ_history_id);
					eq_his.setSourceDataPk(bh.getId());
					eq_his.setSourceTableName("bundle_head");
					eq_his = this.equipmentHistoryRepository.save(eq_his);
				}
		    }
		}
		
		Map<String,Object> item = new HashMap<>();
		item.put("id", bh.getId());
		
		result.data = item;
		
		return result;
	}
	
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteEquipCard(
			@RequestParam(value="bh_id", required=false) Integer bh_id,
			HttpServletRequest request) {
		
		AjaxResult result = new AjaxResult();
		
		List<EquipmentHistory> eq_hisList = this.equipmentHistoryRepository.findBySourceDataPkAndSourceTableName(bh_id, "bundle_head");
		if (eq_hisList.size() > 0) {
			for(int i = 0; i < eq_hisList.size(); i++) {
				EquipmentHistory eq_his = eq_hisList.get(i);
				eq_his.setSourceDataPk(null);
				eq_his.setSourceTableName(null);
				this.equipmentHistoryRepository.save(eq_his);
		    }
		}
		
		this.equipHistoryCardService.deleteEquipHistoryAndEqu(bh_id);
		this.bundleHeadRepository.deleteById(bh_id);
		
		result.success = true;
		return result;
	}
	
	@PostMapping("/equ_available")
	@Transactional
	public AjaxResult updateEquipCard(
			@RequestParam(value="bh_id", required=false) Integer bh_id,
			HttpServletRequest request) {
		
		AjaxResult result = new AjaxResult();
		
		this.equipHistoryCardService.updateEquipHistoryAndEqu(bh_id);
		
		result.success = true;
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
		cell.setCellValue("설비그룹");
		cell = row.createCell(1);
		cell.setCellValue("용도");
		cell = row.createCell(2);
		cell.setCellValue("설비코드");
		cell = row.createCell(3);
		cell.setCellValue("설비명");
		cell = row.createCell(4);
		cell.setCellValue("관리번호");
		cell = row.createCell(5);
		cell.setCellValue("워크센터");
		cell = row.createCell(6);
		cell.setCellValue("모델명");
		cell = row.createCell(7);
		cell.setCellValue("제조사");
		cell = row.createCell(8);
		cell.setCellValue("사용전압");
		cell = row.createCell(9);
		cell.setCellValue("용량(Watt)");
		cell = row.createCell(10);
		cell.setCellValue("입고일");
		cell = row.createCell(11);
		cell.setCellValue("사용부서");
		cell = row.createCell(12);
		cell.setCellValue("AS전화번호");
		cell = row.createCell(13);
		cell.setCellValue("시리얼번호");
		cell = row.createCell(14);
		cell.setCellValue("제작년도");
		cell = row.createCell(15);
		cell.setCellValue("공급업체");
		cell = row.createCell(16);
		cell.setCellValue("구매일");
		cell = row.createCell(17);
		cell.setCellValue("설치일");
		cell = row.createCell(18);
		cell.setCellValue("구매금액(원)");
		cell = row.createCell(19);
		cell.setCellValue("가동률표시여부");
		cell = row.createCell(20);
		cell.setCellValue("폐기일");
		cell = row.createCell(21);
		cell.setCellValue("관리책임자");
		cell = row.createCell(22);
		cell.setCellValue("위치");
		cell = row.createCell(23);
		cell.setCellValue("기타사항");
		cell = row.createCell(24);
		cell.setCellValue("점검 및 사용상 주의점");
		
		Map<String, Object> items = CommonUtil.loadJsonToMap(param.getFirst("param").toString());
		// Body
		
		row = sheet.createRow(rowNum++);
		cell = row.createCell(0);
		cell.setCellValue(items.get("EquipmentGroup_id").toString());
		cell = row.createCell(1);
		cell.setCellValue(items.get("Usage").toString());
		cell = row.createCell(2);
		cell.setCellValue(items.get("Code").toString());
		cell = row.createCell(3);
		cell.setCellValue(items.get("Name").toString());
		cell = row.createCell(4);
		cell.setCellValue(items.get("ManageNumber").toString());
		cell = row.createCell(5);
		cell.setCellValue(items.get("WorkCenter_id").toString());
		cell = row.createCell(6);
		cell.setCellValue(items.get("Model").toString());
		cell = row.createCell(7);
		cell.setCellValue(items.get("Maker").toString());
		cell = row.createCell(8);
		cell.setCellValue(items.get("Voltage").toString());
		cell = row.createCell(9);
		cell.setCellValue(items.get("PowerWatt").toString());
		cell = row.createCell(10);
		cell.setCellValue(items.get("InputDate").toString());
		cell = row.createCell(11);
		cell.setCellValue(items.get("Depart_id").toString());
		cell = row.createCell(12);
		cell.setCellValue(items.get("ASTelNumber").toString());
		cell = row.createCell(13);
		cell.setCellValue(items.get("SerialNumber").toString());
		cell = row.createCell(14);
		cell.setCellValue(items.get("ProductionYear").toString());
		cell = row.createCell(15);
		cell.setCellValue(items.get("SupplierName").toString());
		cell = row.createCell(16);
		cell.setCellValue(items.get("PurchaseDate").toString());
		cell = row.createCell(17);
		cell.setCellValue(items.get("InstallDate").toString());
		cell = row.createCell(18);
		cell.setCellValue(items.get("PurchaseCost").toString());
		cell = row.createCell(19);
		cell.setCellValue(items.get("OperationRateYN").toString());
		cell = row.createCell(20);
		cell.setCellValue(items.get("DisposalDate").toString());
		cell = row.createCell(21);
		cell.setCellValue(items.get("Manager").toString());
		cell = row.createCell(22);
		cell.setCellValue(items.get("Located").toString());
		cell = row.createCell(23);
		cell.setCellValue(items.get("Description").toString());
		cell = row.createCell(24);
		cell.setCellValue(items.get("AttentionRemark").toString());
		
		sheet.autoSizeColumn(0);
		sheet.autoSizeColumn(1);
		sheet.autoSizeColumn(2);
		sheet.autoSizeColumn(3);
		sheet.autoSizeColumn(4);
		sheet.autoSizeColumn(5);
		sheet.autoSizeColumn(6);
		sheet.autoSizeColumn(7);
		sheet.autoSizeColumn(8);
		sheet.autoSizeColumn(9);
		sheet.autoSizeColumn(10);
		sheet.autoSizeColumn(11);
		sheet.autoSizeColumn(12);
		sheet.autoSizeColumn(13);
		sheet.autoSizeColumn(14);
		sheet.autoSizeColumn(15);
		sheet.autoSizeColumn(16);
		sheet.autoSizeColumn(17);
		sheet.autoSizeColumn(18);
		sheet.autoSizeColumn(19);
		sheet.autoSizeColumn(20);
		sheet.autoSizeColumn(21);
		sheet.autoSizeColumn(22);
		sheet.autoSizeColumn(23);
		sheet.autoSizeColumn(24);
		
		// 컨텐츠 타입과 파일명 지정
		String fileName = "설비이력카드";
		
		response.setContentType("ms-vnd/excel;charset=UTF-8");
		response.setHeader("Content-Disposition", "attachment;filename="+ URLEncoder.encode(fileName, "UTF-8") +".xlsx");
	    
		wb.write(response.getOutputStream());
		wb.close();
	}
	
	/*
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveEquipCard(
			@RequestParam(value="bh_id", required=false) Integer bhId, 
    		@RequestParam(value="title", required=false) String title,
    		@RequestParam(value="doc_name", required=false) String docName, 
    		@RequestParam(value="data_date", required=false) String data_date,
    		@RequestParam(value="content", required=false) String content, 
    		@RequestParam(value="id", required=false) Integer docId, 
    		@RequestParam(value="fileId", required=false) String fileId,
    		Authentication auth,
    		HttpServletRequest request
			) {
		
		
		
		Optional<DocForm> df = this.docFormRepository.findByFormName("설비이력카드");
		
		Integer docFormId = null;
		
		if (df.isEmpty() == false) {
			DocForm docForm = df.get();			
			docFormId = docForm.getId();
		}
		
		User user = (User)auth.getPrincipal();
		
		Timestamp dataDate = Timestamp.valueOf(data_date+ " 00:00:00");
		
		AjaxResult result = new AjaxResult();
		
		BundleHead bh = new BundleHead();
		
		if(bhId > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bhId);
		}
		
		bh.setTableName("equip_history_card");
		bh.setChar1(title);
		bh.setDate1(dataDate);
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		DocResult dt = new DocResult();
		
		if(bhId > 0) {
			List<DocResult> docList = this.docResultRepository.findByNumber1AndText1((float)bhId, "equip_history_card");
			dt = docList.get(0);
		} else {
			dt.setText1("equip_history_card");
		}
		
		dt.setNumber1((float)bh.getId());
		dt.setDocumentName(docName);
		dt.setDocumentFormId(docFormId);
		dt.setContent(content);
		dt.setDocumentDate(dataDate);
		dt.set_audit(user);
		
		this.docResultRepository.save(dt);
		
		if (StringUtils.hasText(fileId)) {
			Integer dataPk = dt.getId();
			String[] fileIdList = fileId.split(",");
			
			for (int i = 0; i < fileIdList.length; i++) {
				fileService.updateDataPk(Integer.parseInt(fileIdList[i]), dataPk);
			}
		}
		
		Map<String,Object> item = new HashMap<>();
		item.put("id", bh.getId());
		
		result.data = item;
		
		return result;
	}
	
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteEquipCard(
			@RequestParam(value="bh_id", required=false) Integer bhId,
			HttpServletRequest request) {
		
		AjaxResult result = new AjaxResult();
		
		List<DocResult> dr = this.docResultRepository.findByNumber1AndText1((float)bhId, "equip_history_card");
		
		for (int i = 0; i < dr.size(); i++) {
			this.docResultRepository.deleteById(dr.get(i).getId());
		}
		
		this.bundleHeadRepository.deleteById(bhId);
		
		result.success = true;
		return result;
	}
	*/
}
