package mes.app.precedence;

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

import mes.app.precedence.service.SurfaceBacerialService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.TestItemResult;
import mes.domain.entity.TestMaster;
import mes.domain.entity.TestResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.TestItemResultRepository;
import mes.domain.repository.TestMasterRepository;
import mes.domain.repository.TestResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/surface_bacerial")
public class SurfaceBacerialController {
	@Autowired
	SurfaceBacerialService surfaceBacerialService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	TestItemResultRepository testItemResultRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	TestMasterRepository testMasterRepository;
	
	@Autowired
	TestResultRepository testResultRepository;
	
	
	@GetMapping("/read")
	private AjaxResult Result(
			@RequestParam("start_date") String start_date,
			@RequestParam("end_date") String end_date) {
		
		List<Map<String, Object>> items = this.surfaceBacerialService.getList(start_date, end_date);
		
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
		
		Map<String, Object> items = this.surfaceBacerialService.apprList(bhId,data_date,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	@PostMapping("/save")
	private AjaxResult save(
			@RequestParam(value="bhId", required=false) Integer bhId,
			@RequestParam(value="title", required=false) String title,
			@RequestParam("data_date") String data_date,
			@RequestParam (value="headInfo") String headInfo,
			@RequestParam (value="diaryInfo") String diaryInfo,
			@RequestParam (value="judgment") String judgment,
			@RequestParam (value="pickupDate") String pickupDate,
			Authentication auth,
			HttpServletRequest request) throws JSONException {
		
		User user = (User)auth.getPrincipal();
		
		JSONObject headInfo1 = new JSONObject(headInfo);
		
		Integer bh_id = null;
		
		List<Map<String, Object>> diaryInfo1 = CommonUtil.loadJsonListMap(diaryInfo);
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(data_date+ " 00:00:00");
		
		Timestamp pickupDate1 = Timestamp.valueOf(pickupDate+ " 00:00:00");
		
		
		
		
		String sql = "";
		
		BundleHead bh = new BundleHead();
		
		if(bhId > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bhId);

		}else {
			bh.setDate1(checkDate);
			bh.setTableName("surface_bacerial");
		}
		bh.setChar1(headInfo1.get("Title").toString());
		bh.setText1(judgment);
		bh.set_audit(user);
		this.bundleHeadRepository.save(bh);
		
		bh_id = bh.getId();
		
		List<TestMaster> row =  testMasterRepository.findByName("표면오염도검사");
		
		Integer tmId =  row.get(0).getId();
		
		TestResult tr = new TestResult();
		
		if(bhId > 0) {
			tr = this.testResultRepository.getTestResultBySourceDataPk(bhId);
			
		}else {
			tr.setSourceDataPk(bh_id);
			tr.setSourceTableName("surface_bacerial");
			tr.setTestDateTime(checkDate);
		}
		tr.setTestMasterId(tmId);
		tr.setMaterialId(0);
		tr.set_audit(user);
		this.testResultRepository.save(tr);
		
		Integer trId = tr.getId();
		
		
		if(bhId > 0) {
			for(int i=0; i<diaryInfo1.size(); i++) {
				TestItemResult tir = this.testItemResultRepository.getTestItemResultById((Integer) diaryInfo1.get(i).get("tirId"));
				if(tir != null) {
					tir.setTestItemId((Integer) diaryInfo1.get(i).get("TestItem_id"));
					tir.setChar1(diaryInfo1.get(i).get("Char1").toString());
					tir.setChar2(diaryInfo1.get(i).get("Char2").toString());
					tir.setTestDateTime(pickupDate1);
					tir.setCharResult(diaryInfo1.get(i).get("CharResult").toString());
					tir.setTestResultId(trId);
					tir.set_audit(user);
					
					this.testItemResultRepository.save(tir);
				}else {
					TestItemResult tir1 = new TestItemResult();
					tir1.setSourceDataPk(bh_id);
					tir1.setSourceTableName("surface_bacerial");
					tir1.setTestItemId((Integer) diaryInfo1.get(i).get("TestItem_id"));
					tir1.setChar1(diaryInfo1.get(i).get("Char1").toString());
					tir1.setChar2(diaryInfo1.get(i).get("Char2").toString());
					tir1.setTestDateTime(pickupDate1);
					tir1.setCharResult(diaryInfo1.get(i).get("CharResult").toString());
					tir1.setTestResultId(trId);
					tir1.set_audit(user);
					
					this.testItemResultRepository.save(tir1);
				}
			}
		}else {
			for(int i=0; i<diaryInfo1.size(); i++) {
				TestItemResult tir = new TestItemResult();
				tir.setSourceDataPk(bh_id);
				tir.setSourceTableName("surface_bacerial");
				tir.setTestItemId((Integer) diaryInfo1.get(i).get("TestItem_id"));
				tir.setChar1(diaryInfo1.get(i).get("Char1").toString());
				tir.setChar2(diaryInfo1.get(i).get("Char2").toString());
				tir.setTestDateTime(pickupDate1);
				tir.setCharResult(diaryInfo1.get(i).get("CharResult").toString());
				tir.setTestResultId(trId);
				tir.set_audit(user);
				
				this.testItemResultRepository.save(tir);
			}
		}
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		paramMap.addValue("bhIdSec", bh_id);
		paramMap.addValue("userId", user.getId());
		
		
if(bhId > 0) {
			
			sql = """
		             insert into devi_action("SourceDataPk", "SourceTableName", "HappenDate", "HappenPlace"
					       ,"AbnormalDetail", "ConfirmDetail", _created, _creater_id)
					 select tir.id as src_data_pk, 'surface_bacerial' , to_char(bh."Date1", 'yyyy-MM-dd')::date as happen_date ,'표면오염도 검사성적서' as happen_place
	                      , concat('부적합 : ', mt."Name") as abnormal_detail ,tir."CharResult"     
                        ,current_date, :userId
	                from bundle_head bh
	                inner join test_result tr on bh.id = tr."SourceDataPk" and tr."SourceTableName" = 'surface_bacerial' 
	                inner join test_item_result tir on tir."TestResult_id" = tr.id and tir."Char2" = 'X'
	                inner join test_item mt on tir."TestItem_id" = mt.id
	                and bh."TableName" = 'surface_bacerial'
	                and tr."SourceDataPk" = :bhIdSec
	                and tir.id not in (
			            select a."SourceDataPk" 
			            from devi_action a
			            inner join test_item_result tir on a."SourceDataPk" = tir.id
			            inner join test_result tr on tr.id = tir."TestResult_id" and tr."SourceDataPk" = :bhIdSec
			            where a."SourceTableName" = 'surface_bacerial'
					)
	                order by tir._order
		        		""";
				 
			this.sqlRunner.execute(sql, paramMap);
			
			
			
			sql = """
					delete from devi_action
	                where "SourceDataPk" not in (
		                select tir.id
		                from test_result tr
		                inner join test_item_result tir on tir."TestResult_id" = tr.id  and tir."Char2" = 'X'
		                left join  devi_action c on c."SourceDataPk" = tir.id
		                where 1=1 
	                )
	                and "SourceTableName" = 'surface_bacerial'
	        	  """;
			
			this.sqlRunner.execute(sql, paramMap);
	        
		}else {
			sql = """
		             insert into devi_action("SourceDataPk", "SourceTableName", "HappenDate", "HappenPlace"
					       ,"AbnormalDetail", "ConfirmDetail", _created, _creater_id)
					 select tir.id as src_data_pk, 'surface_bacerial' , to_char(bh."Date1", 'yyyy-MM-dd')::date as happen_date ,'표면오염도 검사성적서' as happen_place
	                      , concat('부적합 : ', mt."Name") as abnormal_detail ,tir."CharResult"     
                        ,current_date, :userId
	                from bundle_head bh
	                inner join test_result tr on bh.id = tr."SourceDataPk" and tr."SourceTableName" = 'surface_bacerial' 
	                inner join test_item_result tir on tir."TestResult_id" = tr.id and tir."Char2" = 'X'
	                inner join test_item mt on tir."TestItem_id" = mt.id
	                and bh."TableName" = 'surface_bacerial'
	                and tr."SourceDataPk" = :bhIdSec
	                and tir.id not in (
			            select a."SourceDataPk" 
			            from devi_action a
			            inner join test_item_result tir on a."SourceDataPk" = tir.id
			            inner join test_result tr on tr.id = tir."TestResult_id" and tr."SourceDataPk" = :bhIdSec
			            where a."SourceTableName" = 'surface_bacerial'
					)
	                order by tir._order
		        		""";
				 
			this.sqlRunner.execute(sql, paramMap);
			
		}
		
		 Map<String, Object> items = new HashMap<>();
	        items.put("id", bh_id);
	        
	        result.data = items;
	        
			return result;
	}
	
	
	@GetMapping("/plusList")
	private AjaxResult plusList(
			@RequestParam(value="bhId", required=false) Integer bhId,
			@RequestParam("data_date") String data_date,
			Authentication auth,
			HttpServletRequest request) {
		
		Map<String, Object> items = this.surfaceBacerialService.plusList(bhId,data_date,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	
	@PostMapping("/delete")
	public AjaxResult mstDelete(
			@RequestParam(value="bhId", required=false) Integer bhId) {
		
		AjaxResult result = new AjaxResult();
		
        this.surfaceBacerialService.mstDelete(bhId);
        
        result.success = true;
        
        return result;
	}
	
}
