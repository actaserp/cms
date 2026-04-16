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

import mes.app.precedence.service.WaterTestService;
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
@RequestMapping("/api/precedence/water_test")
public class WaterTestController {
	@Autowired
	WaterTestService waterTestService;
	
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
		
		List<Map<String, Object>> items = this.waterTestService.getList(start_date, end_date);
		
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
		
		Map<String, Object> items = this.waterTestService.apprList(bhId,data_date,auth);
		
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
		
		List<Map<String, Object>> diaryInfo1 = CommonUtil.loadJsonListMap(diaryInfo);
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(data_date+ " 00:00:00");
		
		Timestamp pickupDate1 = Timestamp.valueOf(pickupDate+ " 00:00:00");
		
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		String sql = "";
		
		BundleHead bh = new BundleHead();
		
		if(bhId > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bhId);

		}else {
			bh.setDate1(checkDate);
			bh.setTableName("water_test");
		}
		bh.setChar1(headInfo1.get("Title").toString());
		bh.setText1(judgment);
		bh.set_audit(user);
		this.bundleHeadRepository.save(bh);
		
		Integer bh_id = bh.getId();
		
		List<TestMaster> row =  testMasterRepository.findByName("용수검사");
		
		Integer tmId =  row.get(0).getId();
		
		TestResult tr = new TestResult();
		
		if(bhId > 0) {
			tr = this.testResultRepository.getTestResultBySourceDataPk(bhId);
			
		}else {
			tr.setSourceDataPk(bh_id);
			tr.setSourceTableName("water_test");
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
				
				tir.setTestItemId((Integer) diaryInfo1.get(i).get("TestItem_id"));
				tir.setChar1(diaryInfo1.get(i).get("Char1").toString());
				tir.setChar2(diaryInfo1.get(i).get("Char2").toString());
				tir.setTestDateTime(pickupDate1);
				tir.setCharResult(diaryInfo1.get(i).get("CharResult").toString());
				tir.setTestResultId(trId);
				tir.set_audit(user);
				
				this.testItemResultRepository.save(tir);
			}
		}else {
			for(int i=0; i<diaryInfo1.size(); i++) {
				TestItemResult tir = new TestItemResult();
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
		
		paramMap.addValue("bhIdSec", bh_id);
		paramMap.addValue("userId", user.getId());
		
		
		if(bhId > 0) {
			
			sql = """
		             insert into devi_action("SourceDataPk", "SourceTableName", "HappenDate", "HappenPlace"
					       ,"AbnormalDetail", "ConfirmDetail", _created, _creater_id)
					 select tir.id as src_data_pk, 'water_test' , to_char(bh."Date1", 'yyyy-MM-dd')::date as happen_date, '용수검사성적서' as happen_place
	                      , concat('부적합 : ', mt."Name") as abnormal_detail,tir."CharResult" 
                        ,current_date, :userId
	                from bundle_head bh
	                inner join test_result tr on bh.id = tr."SourceDataPk" and tr."SourceTableName" = 'water_test' 
	                inner join test_item_result tir on tir."TestResult_id" = tr.id and tir."Char2" = 'X'
	                inner join test_item mt on tir."TestItem_id" = mt.id
	                and bh."TableName" = 'water_test'
	                and tr."SourceDataPk" = :bhIdSec
	                and tir.id not in (
			            select a."SourceDataPk" 
			            from devi_action a
			            inner join test_item_result tir on a."SourceDataPk" = tir.id
			            inner join test_result tr on tr.id = tir."TestResult_id" and tr."SourceDataPk" = :bhIdSec
			            where a."SourceTableName" = 'water_test'
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
	                and "SourceTableName" = 'water_test'
	        	  """;
			
			this.sqlRunner.execute(sql, paramMap);
	        
		}else {
			sql = """
		             insert into devi_action("SourceDataPk", "SourceTableName", "HappenDate", "HappenPlace"
					       ,"AbnormalDetail", "ConfirmDetail", _created, _creater_id)
					 select tir.id as src_data_pk, 'water_test' , to_char(bh."Date1", 'yyyy-MM-dd')::date as happen_date ,'용수검사성적서' as happen_place
	                      , concat('부적합 : ', mt."Name") as abnormal_detail,tir."CharResult"     
                        ,current_date, :userId
	                from bundle_head bh
	                inner join test_result tr on bh.id = tr."SourceDataPk" and tr."SourceTableName" = 'water_test' 
	                inner join test_item_result tir on tir."TestResult_id" = tr.id and tir."Char2" = 'X'
	                inner join test_item mt on tir."TestItem_id" = mt.id
	                and bh."TableName" = 'water_test'
	                and tr."SourceDataPk" = :bhIdSec
	                and tir.id not in (
			            select a."SourceDataPk" 
			            from devi_action a
			            inner join test_item_result tir on a."SourceDataPk" = tir.id
			            inner join test_result tr on tr.id = tir."TestResult_id" and tr."SourceDataPk" = :bhIdSec
			            where a."SourceTableName" = 'water_test'
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
		
		Map<String, Object> items = this.waterTestService.plusList(bhId,data_date,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	
	@PostMapping("/delete")
	public AjaxResult mstDelete(
			@RequestParam(value="bhId", required=false) Integer bhId) {
		
		AjaxResult result = new AjaxResult();
		
        this.waterTestService.mstDelete(bhId);
        
        result.success = true;
        
        return result;
	}
}
