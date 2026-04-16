package mes.app.precedence;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.DeviActionService;
import mes.app.common.service.FileService;
import mes.app.precedence.service.ReservoirCleanStatService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.CheckItemResult;
import mes.domain.entity.CheckResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/reservoir_clean_stat")
public class ReservoirCleanStatController {

	@Autowired
	private ReservoirCleanStatService reservoirCleanStatService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	@Autowired
	CheckItemResultRepository checkItemResultRepository;
	
	@Autowired
	DeviActionService deviActionService;
	
	@Autowired
	FileService fileService;
	
	@Autowired
	SqlRunner sqlRunner;
	
	@GetMapping("/appr_stat")
	public AjaxResult getApprStat(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="appr_status", required=false) String apprStatus
			) {
		
        List<Map<String, Object>> items = this.reservoirCleanStatService.getApprStat(startDate,endDate,apprStatus);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/read")
	public AjaxResult getReservoirClean(
			@RequestParam(value="bh_id", required=false) Integer bhId,
			@RequestParam(value="data_date", required=false) String dataDate, 
			@RequestParam(value="end_date", required=false) String endDate,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		Map<String, Object> items = this.reservoirCleanStatService.getReservoirClean(bhId,dataDate,endDate,user);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveReservoirClean(
			@RequestParam(value="bh_id", required=false) String bh_id,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="headInfo", required=false) String headInfo,
			@RequestParam(value="diaryInfo", required=false) String diaryInfo,
			@RequestParam(value="fileId", required=false) String fileId,
			HttpServletRequest request,
			Authentication auth) throws JSONException {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp dataDate = Timestamp.valueOf(data_date+ " 00:00:00");
		
		JSONObject headInfos = new JSONObject(headInfo);
		
		BundleHead bh = new BundleHead();
		
		Integer bhId = Integer.parseInt(bh_id);
		
		if (bhId > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bhId);
		} else {
			bh.setDate1(dataDate);
			String checkMasterId = headInfos.get("CheckMaster_id").toString();
			bh.setNumber1(Float.parseFloat(checkMasterId));
			bh.setTableName("check_result_reservoir_clean");
		}
		
		bh.setChar1(title);
		bh.setText1(headInfos.get("content").toString());
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		bhId = bh.getId();
		
		CheckResult cr = new CheckResult();
		
		Integer checkResultId = Integer.parseInt(headInfos.get("check_result_id").toString());
		
		if (checkResultId > 0) {
			cr = this.checkResultRepository.getCheckResultById(checkResultId);
		} else {
			cr.setSourceDataPk(bhId);
			cr.setCheckMasterId(Integer.parseInt(headInfos.get("CheckMaster_id").toString()));
			cr.setCheckDate(Date.valueOf(data_date));
			cr.setChar1("reservior_clean");
		}
		
		cr.setDescription(headInfos.get("Description").toString());
		cr.set_audit(user);
		
		this.checkResultRepository.save(cr);
		
		checkResultId = cr.getId();
		
		List<Map<String, Object>> diaryInfos = CommonUtil.loadJsonListMap(diaryInfo);
		
		for(int i = 0; i < diaryInfos.size(); i++) {
			Integer id = diaryInfos.get(i).get("id") == null ? 0 : Integer.parseInt(diaryInfos.get(i).get("id").toString());
			CheckItemResult ci = new CheckItemResult();
			if(id > 0) {
				ci = this.checkItemResultRepository.getCheckItemResultById(id);
			} else {
				ci.setCheckResultId(checkResultId);
				ci.setCheckItemId(Integer.parseInt(diaryInfos.get(i).get("item_id").toString()));
				ci.setOrder(Integer.parseInt(diaryInfos.get(i).get("_order").toString()));
			}
			
			String result1 = diaryInfos.get(i).get("result1") == null ? null : diaryInfos.get(i).get("result1").toString();
			ci.setResult1(result1);
			ci.set_audit(user);
			
			this.checkItemResultRepository.save(ci);
		}
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		paramMap.addValue("userId", user.getId());
		
		String sql = """
					select ir.id as src_data_pk, 'devi_action_reservoir_clean' as source_table_name
					, to_char(b."Date1", 'yyyy-MM-dd') as happen_date ,cm."Name" as happen_place                
					, concat('부적합 : ', coalesce(ci."ItemGroup1",' '), coalesce(ci."ItemGroup2", ' '), coalesce(ci."ItemGroup3",' '), ci."Name") as abnormal_detail       
					,null as action_detail ,null as confirm_detail, current_date , :userId
					from bundle_head b
					inner join check_result cr on b.id = cr."SourceDataPk"
					inner join check_item_result ir on cr.id = ir."CheckResult_id" and ir."Result1" = 'X'
					inner join check_item ci on ir."CheckItem_id" = ci.id
					inner join check_mast cm on cm.id = b."Number1"
					where cr."SourceDataPk" = :bhId
					and ir.id not in (
					select a."SourceDataPk" 
					from devi_action a
					inner join check_item_result b on a."SourceDataPk" = b.id
					inner join check_result cr on b."CheckResult_id" = cr.id and cr."SourceDataPk" = :bhId 
					where a."SourceTableName" = 'devi_action_reservoir_clean'
					)
					order by ir._order
					""";
		List<Map<String, Object>> devi = this.sqlRunner.getRows(sql, paramMap);
		
		if (devi.size() > 0) {
			for (int i = 0; i < devi.size(); i++) {
				deviActionService.saveDeviAction(0, 
						Integer.parseInt(devi.get(i).get("src_data_pk").toString())
						, devi.get(i).get("source_table_name").toString()
						, devi.get(i).get("happen_date").toString()
						, devi.get(i).get("happen_place").toString()
						, devi.get(i).get("abnormal_detail").toString()
						, devi.get(i).get("action_detail") == null ? null : devi.get(i).get("action_detail").toString()
						, devi.get(i).get("confirm_detail") == null ? null : devi.get(i).get("confirm_detail").toString()
						, user);
			}
		}
		
		sql = """
		       select id from devi_action
                where "SourceDataPk" not in (
	                select b.id
	                from check_result a
	                inner join check_item_result b on a.id = b."CheckResult_id" and b."Result1" = 'X'
	                left join  devi_action c on c."SourceDataPk" = b.id
	                where 1=1	
                )
                and "SourceTableName" = 'devi_action_reservoir_clean'
			  """;
		
		List<Map<String,Object>> delDevi = this.sqlRunner.getRows(sql, paramMap);
		
		if (delDevi.size() > 0) {
			for (int i = 0; i< delDevi.size(); i++) {
				deviActionService.deleteDeviAction(Integer.parseInt(delDevi.get(i).get("id").toString()));
			}
		}
		
		if (StringUtils.hasText(fileId)) {
			Integer dataPk = bhId;
			String[] fildIdList = fileId.split(",");
			
			for (int i = 0; i < fildIdList.length; i++) {
				fileService.updateDataPk(Integer.parseInt(fildIdList[i]), dataPk);
			}
		}
		
		Map<String, Object> item = new HashMap<>();
		item.put("id", bhId);
		
		result.data = item;
		
		return result;
	}
	
	@PostMapping("/mst_delete")
	public AjaxResult mstDelete(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		this.reservoirCleanStatService.mstDelete(bhId);
		
		result.success = true;
		
		return result;
	}
}
