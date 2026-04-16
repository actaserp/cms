package mes.app.haccp.verification_checklist_haccp_normal;

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.haccp.verification_checklist_haccp_normal.service.VerifiCheckHaccpNormalService;
import mes.domain.entity.AttachFile;
import mes.domain.entity.BundleHead;
import mes.domain.entity.CheckItemResult;
import mes.domain.entity.CheckResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.AttachFileRepository;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.repository.CheckMasterRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/haccp/verifi_check_haccp_normal")
public class VerifiCheckHaccpNormalController {
	
	@Autowired
	private VerifiCheckHaccpNormalService verifiCheckHaccpNormalService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	@Autowired
	CheckItemResultRepository checkItemResultRepository;
	
	@Autowired
	CheckMasterRepository checkMasterRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	private FileService fileService;
	
	@Autowired
	AttachFileRepository attachFileRepository;

	// 결재현황 조회
	@GetMapping("/appr_stat")
	public AjaxResult getVerifiCheckHaccpNormalApprStatus(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="appr_state", required=false) String apprState,
			@RequestParam(value="verifi_type", required=false) String verifiType,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.verifiCheckHaccpNormalService.getVerifiCheckHaccpNormalApprStatus(startDate,endDate,apprState,verifiType);     
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 점검표 조회
	@GetMapping("/read")
	public AjaxResult getVerifiCheckHaccpNormalList(
    		@RequestParam(value="bh_id", required=false) Integer bh_id, 
			HttpServletRequest request) {
		
        Map<String, Object> items = this.verifiCheckHaccpNormalService.getVerifiCheckHaccpNormalList(bh_id);      
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
		
	//최초 등록
	@PostMapping("/insert")
	@Transactional
	public AjaxResult insertVerifiCheckHaccpNormalList(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bh_id,
			@RequestParam(value="check_master_id", required=false) Integer check_master_id,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam(value = "fileId", required = false) String file_id,
			@RequestParam(value="verifi_type", required=false) String verifi_type,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
				
		BundleHead bh = new BundleHead();
		bh.setTableName("verification_checklist_haccp_normal");
		bh.setChar1(title);
		bh.setDate1(CommonUtil.tryTimestamp(data_date));
		bh.setNumber1((float)check_master_id);
		bh.setText1(verifi_type);
		bh.set_audit(user);
		bh = this.bundleHeadRepository.save(bh);
		Integer bhId = bh.getId();
		
		CheckResult cr = new CheckResult();
		cr.setCheckMasterId(check_master_id);
		cr.setCheckDate(Date.valueOf(data_date));    
		cr.setCheckerName(user.getUserProfile().getName());
		cr.setSourceDataPk(bhId);
		cr.set_audit(user);
		cr = this.checkResultRepository.save(cr);
		Integer check_result_id = cr.getId();
		
		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.size() == 0) {
			result.success = false;
			return result;
		}
		
		for(int i = 0; i < qItems.size(); i++) {
			Integer check_item_id = Integer.parseInt(qItems.get(i).get("id").toString());
	    	String result1 = CommonUtil.tryString(qItems.get(i).get("result1"));
	    	
	    	CheckItemResult cir = new CheckItemResult();
	    	cir.setCheckResultId(check_result_id);
	    	cir.setCheckItemId(check_item_id);
	    	cir.setResult1(result1);
	        cir.set_audit(user);
	        this.checkItemResultRepository.save(cir);
	    }
		
		if ( StringUtils.hasText(file_id) )  {
			Integer data_pk = bh.getId();
			String[] fileIdList = file_id.split(",");
			
			for (String fileId : fileIdList) {
				int _id = Integer.parseInt(fileId);
				this.fileService.updateDataPk(_id, data_pk);
			}
		}
		
		Map<String, Object> item = new HashMap<String, Object>();
		item.put("id", bhId);
		result.data = item;
		return result;
	}
	
	// 수정 저장
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveVerifiCheckHaccpNormalList(
			@RequestParam("bh_id") Integer bh_id,
			@RequestParam(value="check_master_id", required=false) Integer check_master_id,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam(value = "fileId", required = false) String file_id,
			@RequestParam(value="verifi_type", required=false) String verifi_type,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		BundleHead bh = this.bundleHeadRepository.getBundleHeadById(bh_id);
		Integer bhId = bh.getId();
		
		CheckResult cr = this.checkResultRepository.getCheckResultBySourceDataPk(bhId);
		Integer check_result_id = cr.getId();
		
		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.size() == 0) {
			result.success = false;
			return result;
		}
		
		for(int i = 0; i < qItems.size(); i++) {
			Integer check_item_id = Integer.parseInt(qItems.get(i).get("id").toString());
	    	String result1 = CommonUtil.tryString(qItems.get(i).get("result1"));
	    	
	    	CheckItemResult cir = this.checkItemResultRepository.findByCheckResultIdAndCheckItemId(check_result_id, check_item_id);
	    	cir.setResult1(result1);
	        cir.set_audit(user);
	        this.checkItemResultRepository.save(cir);
	    }
		
		if ( StringUtils.hasText(file_id) )  {
			Integer data_pk = bh.getId();
			String[] fileIdList = file_id.split(",");
			
			for (String fileId : fileIdList) {
				int _id = Integer.parseInt(fileId);
				this.fileService.updateDataPk(_id, data_pk);
			}
		}
		
		Map<String, Object> item = new HashMap<String, Object>();
		item.put("id", bhId);
		result.data = item;
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteVerifiCheckHaccpNormalList(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bh_id,
			@RequestParam(value="table_name", required=false) String table_name,
			@RequestParam(value="attach_name", required=false) String attach_name) {
		
		AjaxResult result = new AjaxResult();
		
		this.verifiCheckHaccpNormalService.deleteVerifiCheckHaccpNormalList(bh_id);
		
		List<AttachFile> afList = this.attachFileRepository.getAttachFileByTableNameAndDataPkAndAttachName(table_name,bh_id,attach_name);
		if(afList.size()>0) {
			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("bh_id", bh_id);
			paramMap.addValue("table_name", table_name);
			paramMap.addValue("attach_name", attach_name);
			
			String sql = """
				   delete from attach_file 
				   where "DataPk" = :bh_id 
				   and "TableName" = :table_name
				   and "AttachName" = :attach_name
				  """;
				
			this.sqlRunner.execute(sql, paramMap);
		}
		
		result.success = true;
		
		return result;
	}
}
