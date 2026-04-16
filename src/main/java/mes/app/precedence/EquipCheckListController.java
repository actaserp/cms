package mes.app.precedence;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.precedence.service.EquipCheckListService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.CheckItemResult;
import mes.domain.entity.CheckMaster;
import mes.domain.entity.CheckResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.repository.CheckMasterRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/equip_check_list")
public class EquipCheckListController {
	
	@Autowired
	private EquipCheckListService equipCheckListService;
	
	@Autowired
	private CheckMasterRepository checkMasterRepository;
	
	@Autowired
	private BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	private CheckResultRepository checkResultRepository;
	
	@Autowired
	private CheckItemResultRepository checkItemResultRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	
	// 설비점검현황 조회
	@GetMapping("/appr_stat")
	public AjaxResult getApprStat(
    		@RequestParam(value="check_master_id", required=false) Integer checkMasterId, 
    		@RequestParam(value="start_date", required=false) String startDate, 
    		@RequestParam(value="end_date", required=false) String endDate, 
    		@RequestParam(value="appr_state", required=false) String apprState,
			HttpServletRequest request) {
		
		Map<String, Object> items = this.equipCheckListService.getApprStat(checkMasterId,startDate,endDate,apprState);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
		
	}

	// 설비점검일지 조회
	@GetMapping("/read")
	public AjaxResult getEquipCheckList(
    		@RequestParam(value="bh_id", required=false) Integer bhId, 
			HttpServletRequest request) {
		
		Map<String, Object> items = this.equipCheckListService.getEquipCheckList(bhId);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
		
	}
	
	@GetMapping("/appr_list")
	public AjaxResult getApprList(
			@RequestParam(value="check_master_id", required=false) Integer checkMasterId,
			HttpServletRequest request) {
		
		
		Map<String, Object> item = new HashMap<String,Object>();
		
		if (checkMasterId != null) {
			List<CheckMaster> cmList = checkMasterRepository.findCheckMasterById(checkMasterId);
			
			if(cmList.size() > 0) {
				item.put("code", cmList.get(0).getCode());
			}
		}
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}
	
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveEquipCheckList(
			@RequestParam("bh_id") Integer bhId,
			@RequestParam("title") String title,
			@RequestParam("check_master_id") String checkMasterId,
			@RequestParam("data_date") String data_date,
			@RequestParam("start_date") String startDate,
			@RequestParam("end_date") String endDate,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth	
	) {
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		BundleHead bh = new BundleHead();
		
		Timestamp dataDate = CommonUtil.tryTimestamp(data_date);
		
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		if (bhId > 0) {
			bh = bundleHeadRepository.getBundleHeadById(bhId);
			bh.setTableName("prod_equip_check_list");
			bh.setDate1(dataDate);
			bh.setChar1(title);
			bh.setChar2(startDate);
			bh.setChar3(endDate);
			bh.setText1(checkMasterId);
			
			bh.set_audit(user);
			this.bundleHeadRepository.saveAndFlush(bh);
			
			
			List<CheckResult> cr = this.checkResultRepository.findBySourceDataPk(bhId);
			

			if(cr.size() > 0) {
				Integer checkResultId = cr.get(0).getId();
				for(int i = 0; i < items.size(); i++ ) {
					Integer pk = Integer.parseInt(items.get(i).get("id").toString());
					CheckItemResult cir = this.checkItemResultRepository.findByCheckResultIdAndCheckItemId(checkResultId, pk);
					String result1 = items.get(i).get("result1").toString();
					
					cir.setResult1(result1);
					
					this.checkItemResultRepository.saveAndFlush(cir);
				}
			
			
			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("checkResultId", checkResultId);
			paramMap.addValue("bhId", bhId);
			paramMap.addValue("userId", user.getId());
			
			String sql = """
                insert into devi_action("SourceDataPk", "SourceTableName", "HappenDate", "HappenPlace"
	            , "AbnormalDetail", _created, _creater_id)
                select cir.id as src_data_pk ,'prod_equip_check_list' , cr."CheckDate" as happen_date, cm."Name" as happen_place
	            , concat('부적합 : ', coalesce(ci."ItemGroup1",' '), coalesce(ci."ItemGroup2", ' '), coalesce(ci."ItemGroup3",' '), ci."Name") as abnormal_detail
	            ,current_date, :userId
	            from check_result cr
	            inner join check_item_result cir on cir."CheckResult_id" = cr."id"
	            inner join check_item ci on ci.id = cir."CheckItem_id"
	            inner join check_mast cm on cm.id = cr."CheckMaster_id"	
	            where cr."id" = :checkResultId
                and cir.id not in (
	            select a."SourceDataPk"
	            from devi_action a
                inner join check_item_result b on a."SourceDataPk" = b.id
                inner join check_result c on b."CheckResult_id" = c.id and c."SourceDataPk" = :bhId
	            where a."SourceTableName" = 'prod_equip_check_list' )
                and cir."Result1" = 'X' 
				""";
  
			this.sqlRunner.execute(sql, paramMap);
			
			String sql2 = """
                   delete from devi_action
                    where "SourceTableName" = 'prod_equip_check_list'
                    and "SourceDataPk" not in (
                    select b.id
	                from check_result a
	                inner join check_item_result b on a.id = b."CheckResult_id" and b."Result1" = 'X'
	                left join  devi_action c on c."SourceDataPk" = b.id
	                where 1=1
                    and "Result1" = 'X' )
					""";
			
			this.sqlRunner.execute(sql2, paramMap);
			
			}
			
		} else {
			bh.setTableName("prod_equip_check_list");
			bh.setDate1(dataDate);
			bh.setChar1(title);
			bh.setChar2(startDate);
			bh.setChar3(endDate);
			bh.setText1(checkMasterId);
			
			bh.set_audit(user);
			
			this.bundleHeadRepository.saveAndFlush(bh);
			
			CheckResult cr = new CheckResult();
			cr.setCheckMasterId(Integer.parseInt(checkMasterId));
			cr.setCheckDate(Date.valueOf(data_date));
			cr.setCheckerName(user.getUserProfile().getName());
			cr.setSourceDataPk(bh.getId());
			cr.set_audit(user);
			
			this.checkResultRepository.saveAndFlush(cr);
			
			Integer checkResultId = cr.getId();



			for(int i = 0; i < items.size(); i++ ) {
				CheckItemResult cir = new CheckItemResult();
				cir.setCheckResultId(checkResultId);
				cir.setCheckItemId(Integer.parseInt(items.get(i).get("id").toString()));
				cir.setResult1(items.get(i).get("result1").toString());

				// "order" 값이 null인 경우 기본값 설정
				String orderValue = items.get(i).get("order") != null ? items.get(i).get("order").toString() : "0";
				cir.setOrder(Integer.parseInt(orderValue));

				cir.set_audit(user);
				this.checkItemResultRepository.saveAndFlush(cir);
				
			}
			
			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("checkResultId", checkResultId);
			paramMap.addValue("userId", user.getId());
			
			String sql = """
                insert into devi_action("SourceDataPk", "SourceTableName", "HappenDate", "HappenPlace"
	            , "AbnormalDetail", _created, _creater_id)
                select cir.id as src_data_pk ,'prod_equip_check_list' , cr."CheckDate" as happen_date, cm."Name" as happen_place
	            , concat('부적합 : ', coalesce(ci."ItemGroup1",' '), coalesce(ci."ItemGroup2", ' '), coalesce(ci."ItemGroup3",' '), ci."Name") as abnormal_detail
	            ,current_date, :userId
	            from check_result cr
	            inner join check_item_result cir on cir."CheckResult_id" = cr."id"
	            inner join check_item ci on ci.id = cir."CheckItem_id"
	            inner join check_mast cm on cm.id = cr."CheckMaster_id"	
	            where cr."id" = :checkResultId
                and cir."Result1" = 'X'
				""";
			
			this.sqlRunner.execute(sql, paramMap);
			
		}
		Map<String, Object> item = new HashMap<String,Object>();
		
		item.put("id", bh.getId());
		
		result.data = item;
		
		return result;
	}
	
	@PostMapping("/mst_delete")
	public AjaxResult mstDelete(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		this.equipCheckListService.mstDelete(bhId);
		
		result.success = true;
		
		return result;
	}
}
