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
import mes.app.precedence.service.AirFilterCheckResultService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.MasterResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.MasterResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/air_filter_check_result")
public class AirFilterCheckResultController {

	@Autowired
	private AirFilterCheckResultService airFilterCheckResultService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	MasterResultRepository masterResultRepository;
	
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
    		@RequestParam(value="appr_status", required=false) String apprStatus,
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.airFilterCheckResultService.getApprStat(startDate,endDate,apprStatus);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@GetMapping("/read")
	public AjaxResult getAirFilterRead(
			@RequestParam(value="bh_id", required=false) Integer bhId, 
    		@RequestParam(value="data_date", required=false) String dataDate,
    		Authentication auth,
    		HttpServletRequest request
			) {
		
		Map<String, Object> items = this.airFilterCheckResultService.getAirFilterRead(bhId,dataDate,auth);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveAirFilter(
			@RequestParam(value="bh_id", required=false) Integer bhId, 
    		@RequestParam(value="title", required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date, 
			@RequestParam (value="headInfo") String headInfo,
			@RequestParam (value="diaryInfo") String diaryInfo,
			@RequestParam(value="fileId", required=false) String fileId,
    		Authentication auth,
    		HttpServletRequest request
			) throws JSONException {
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		Timestamp dataDate = Timestamp.valueOf(data_date+ "-01 00:00:00");
		
		JSONObject json = new JSONObject(headInfo);
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(diaryInfo);
		
		BundleHead bh = new BundleHead(); 
		 
		 if (bhId > 0) {
			 bh = this.bundleHeadRepository.getBundleHeadById(bhId);
		 } else {
			 bh.setTableName("master_result_air_filter");
			 bh.setDate1(dataDate);
		 }
		 bh.setChar1(title);
		 bh.setChar2((String)json.get("Description"));
		 bh.set_audit(user);
		 
		 this.bundleHeadRepository.save(bh);
		 
		 bhId = bh.getId();
		 
		 for (int i = 0; i < data.size(); i++) {
			 if (data.get(i).get("check_plan") != null) {
				 MasterResult mr = new MasterResult();
				 if (data.get(i).get("master_result_id") != null) {
					 Integer mrId = Integer.parseInt(data.get(i).get("master_result_id").toString());
					 mr = this.masterResultRepository.getMasterResultById(mrId);
				 } else {
					 mr.setMasterClass("master_t");
					 mr.setMasterTableId(Integer.parseInt(data.get(i).get("id").toString()));
					 mr.setSourceDataPk(bhId);
					 mr.setSourceTableName("bundle_head");
				 }
				 mr.setDataDate(Date.valueOf(data_date + "-01"));
				 mr.setChar1(data.get(i).get("result1") != null ? data.get(i).get("result1").toString() :  null);
				 mr.set_audit(user);
				 
				 this.masterResultRepository.save(mr);
			 }
		 }
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("bhId", bhId);
		
        String sql = """
                select mr.id as src_data_pk
                , bh."TableName" as source_table_name
                , to_char(bh."Date1", 'yyyy-MM-dd') as happen_date
                , '공조필터 점검일지' as happen_place
                , concat('부적합 : ', mt."Name") as abnormal_detail
                , null as action_detail
                , null as confirm_detail
                from bundle_head bh
                inner join master_result mr on bh.id = mr."SourceDataPk" and mr."SourceTableName" = 'bundle_head' and mr."Char1" = 'X'
                inner join master_t mt on mr."MasterTable_id" = mt.id
                and bh."TableName" = 'master_result_air_filter'
                and mr."SourceDataPk" = :bhId
                and mr.id not in (
	            select a."SourceDataPk" 
	            from devi_action a
	            inner join master_result mr on a."SourceDataPk" = mr.id 
	            where a."SourceTableName" = 'master_result_air_filter'
				)
                order by mt._order
        		""";
		 
        List<Map<String, Object>> devi = this.sqlRunner.getRows(sql, dicParam);
        
        if(devi.size() > 0) {
        	for (int i = 0; i < devi.size(); i++) {
        		Integer sourcePk = devi.get(i).get("src_data_pk") != null ? Integer.parseInt(devi.get(i).get("src_data_pk").toString()) : null;
        		String sourceTableName =  devi.get(i).get("source_table_name") != null ? devi.get(i).get("source_table_name").toString() : "";
        		String happenDate  =  devi.get(i).get("happen_date") != null ? devi.get(i).get("happen_date").toString() : "";
        		String happenPlace  =  devi.get(i).get("happen_place") != null ? devi.get(i).get("happen_place").toString() : "";
        		String abnormalDetail  =  devi.get(i).get("abnormal_detail") != null ? devi.get(i).get("abnormal_detail").toString() : "";
        		String actionDetail  =  devi.get(i).get("action_detail") != null ? devi.get(i).get("action_detail").toString() : "";
        		String confirmDetail  =  devi.get(i).get("confirm_detail") != null ? devi.get(i).get("confirm_detail").toString() : "";
        		
        		deviActionService.saveDeviAction(0, sourcePk,sourceTableName,happenDate,happenPlace,abnormalDetail,actionDetail,confirmDetail, user);
        	}
        }
        
        sql = """
        		select id from devi_action
                where "SourceDataPk" not in (
	                select mr.id
	                from master_result mr
	                left join  devi_action c on c."SourceDataPk" = mr.id
	                where mr."Char1" = 'X'	
                )
                and "SourceTableName" = 'master_result_air_filter'
        	  """;
        
        List<Map<String, Object>> del_devi = this.sqlRunner.getRows(sql, dicParam);
        
        if (del_devi.size() > 0) {
        	for (int i = 0; i < del_devi.size(); i++) {
        		deviActionService.deleteDeviAction(Integer.parseInt(del_devi.get(i).get("id").toString()));
        	}
        }
        
		if (StringUtils.hasText(fileId)) {
			Integer dataPk = bhId;
			String[] fileIdList = fileId.split(",");
			
			for (int i = 0; i < fileIdList.length; i++) {
				fileService.updateDataPk(Integer.parseInt(fileIdList[i]), dataPk);
			}
		}
		
        Map<String, Object> items = new HashMap<>();
        items.put("id", bhId);
        
        result.data = items;
        
		return result;
	}
	
	@PostMapping("/mst_delete")
	public AjaxResult mstDelete(
			@RequestParam(value="bh_id", required=false) Integer bhId) {
		
		AjaxResult result = new AjaxResult();
		
        this.airFilterCheckResultService.mstDelete(bhId);
        
        result.success = true;
        
        return result;
	}
	
}
