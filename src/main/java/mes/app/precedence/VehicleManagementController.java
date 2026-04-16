package mes.app.precedence;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.precedence.service.VehicleManagementService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.CheckItemResult;
import mes.domain.entity.CheckResult;
//import mes.domain.entity.DocResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.repository.DocResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/vehicle_management")
public class VehicleManagementController {
	
	
	@Autowired
	VehicleManagementService vehicleManagementService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	@Autowired
	CheckItemResultRepository checkItemResultRepository;
	
	@Autowired
	DocResultRepository docResultRepository;
	
	@Autowired
	FileService fileService;
	
	@Autowired
	SqlRunner sqlRunner;
	
	
	@GetMapping("/read")
	private AjaxResult VehicleManagementResult(
			@RequestParam("start_date") String start_date,
			@RequestParam("end_date") String end_date) {
		
		List<Map<String, Object>> items = this.vehicleManagementService.getList(start_date, end_date);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	@GetMapping("/ListRead")
	private AjaxResult apprList(
			@RequestParam(value="bhId", required=false) Integer bhId,
			@RequestParam("data_date") String data_date,
			Authentication auth,
			HttpServletRequest request) {
		
		Map<String, Object> items = this.vehicleManagementService.apprList(bhId,data_date,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	

	@PostMapping("/save")
	private AjaxResult save(
			@RequestParam(value="bhId", required=false) Integer bh_Id,
			@RequestParam(value="title",required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam (value="headInfo") String headInfo,
			@RequestParam (value="diaryInfo") String diaryInfo,
			@RequestParam(value="fileId", required=false) String file_id,
			Authentication auth,
			HttpServletRequest request) throws JSONException {
		
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		JSONObject headInfo1 = new JSONObject(headInfo);
		
		Timestamp checkDate = Timestamp.valueOf(data_date+ " 00:00:00");
		
		List<Map<String, Object>> diaryInfo1 = CommonUtil.loadJsonListMap(diaryInfo);
		
		Integer checkResultId=0;
		
		BundleHead bh = new BundleHead(); 
		
		
		if(bh_Id > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bh_Id);
			checkResultId = (Integer) diaryInfo1.get(0).get("checkresult_id");
		}else {
			bh.setTableName("vehicle_management");
			bh.setDate1(checkDate);
			checkResultId = (Integer) headInfo1.get("check_result_id");
		}
		bh.setChar1(title);
		bh.set_audit(user);
		bh.setNumber1((float) 32);
		
		this.bundleHeadRepository.save(bh);
		
		Integer bhId = bh.getId();
		
		// 파일 업데이트
        if (file_id != null && !file_id.isEmpty())  {
			
			Integer data_pk = bhId;
			String[] fileIdList = file_id.split(",");
			
			for (String fileId : fileIdList) {
				int id = Integer.parseInt(fileId);
				this.fileService.updateDataPk(id, data_pk);
			}
		}
		
		
		CheckResult cr = new CheckResult();
		if(checkResultId > 0) {
			cr = this.checkResultRepository.getCheckResultById(checkResultId);
		}else {
			cr.setSourceDataPk(bhId);
			cr.setCheckMasterId(headInfo1.getInt("CheckMaster_id"));
		}
		cr.setCheckDate(Date.valueOf((String) headInfo1.get("DataDate")));
		cr.setCheckerId(user.getId());
		cr.setCheckerName(user.getUserProfile().getName());
		cr.set_audit(user);
		this.checkResultRepository.save(cr);
		headInfo1.put("check_result_id",cr.getId());
			
			
		for(int i=0; i<diaryInfo1.size(); i++) {	
			CheckItemResult cir = new CheckItemResult();
			if(diaryInfo1.get(i).get("id") != null) {
				cir = this.checkItemResultRepository.getCheckItemResultById((Integer)diaryInfo1.get(i).get("id"));
			}else {
				cir.setCheckResultId((Integer)headInfo1.get("check_result_id"));
				cir.setCheckItemId((Integer)diaryInfo1.get(i).get("item_id"));
			}
			cir.setResult1((String) diaryInfo1.get(i).get("result1"));
			cir.set_created(checkDate);
			cir.set_audit(user);
			this.checkItemResultRepository.save(cir);
		}
		
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		paramMap.addValue("userId", user.getId());
		
		String sql = null;
		
		sql = """
                insert into devi_action("SourceDataPk", "SourceTableName", "HappenDate", "HappenPlace"
	            , "AbnormalDetail", _created, _creater_id)
                select ir.id as src_data_pk, 'check_item_devi_result' , cast(to_char(b."Date1", 'yyyy-MM-dd') as date) as happen_date ,'차량관리대장 등록' as happen_place
	            , concat('부적합 : ', coalesce(ci."ItemGroup1",' '), coalesce(ci."ItemGroup2", ' '), coalesce(ci."ItemGroup3",' '), ci."Name") as abnormal_detail
                , current_date, :userId
                from bundle_head b
                inner join check_result cr on b.id = cr."SourceDataPk"
                inner join check_item_result ir on cr.id = ir."CheckResult_id" and ir."Result1" = 'X' 
                inner join check_item ci on ir."CheckItem_id" = ci.id
                inner join check_mast cm on cm.id = b."Number1"
                where cr."SourceDataPk" =  :bhId 
                and ir.id not in (
	                select a."SourceDataPk"
	                from devi_action a
	                inner join check_item_result b on a."SourceDataPk" = b.id
	                inner join check_result cr on b."CheckResult_id" = cr.id and cr."SourceDataPk" = :bhId
	                where a."SourceTableName" = 'check_item_devi_result'	
                )
                order by ir._order
                

				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		if(bh_Id > 0) {
			sql = """
				    delete from devi_action
	                where "SourceDataPk" not in (
		                select b.id
		                from check_result a
		                inner join check_item_result b on a.id = b."CheckResult_id" and b."Result1" = 'X'
		                left join  devi_action c on c."SourceDataPk" = b.id
		                where 1=1	
	                )
	                and "SourceTableName" = 'check_item_devi_result'
				  """;
			
		this.sqlRunner.execute(sql, paramMap);
		}
		Map<String, Object> value = new HashMap<String, Object>();
		value.put("id", bhId);
		
		result.data = value;
		
		
		
		return result;
	}
	
	
	@PostMapping("/delete")
	private AjaxResult delete(
			@RequestParam(value="bhId", required=false) Integer bhId,
			Authentication auth,
			HttpServletRequest request) {
		
		Map<String, Object> items = this.vehicleManagementService.delete(bhId);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	

}
