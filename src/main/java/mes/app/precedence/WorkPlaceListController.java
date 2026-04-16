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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.precedence.service.WorkPlaceListService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.MasterResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.MasterResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/clean/workplace_list")
public class WorkPlaceListController {

	@Autowired
	private WorkPlaceListService workPlaceListService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	MasterResultRepository masterResultRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	@GetMapping("/read_list")
	public AjaxResult getWorkPlaceList(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate,
			HttpServletRequest request) {
		
		List<Map<String,Object>> items = this.workPlaceListService.getWorkPlaceList(startDate,endDate);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@GetMapping("/result_list")
	public AjaxResult getResultList(
			@RequestParam(value="bh_id", required=false) Integer bhId,
			HttpServletRequest request) {
		
		Map<String, Object> items = this.workPlaceListService.getResultList(bhId);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	
	@SuppressWarnings("unchecked")
	@PostMapping("/insert")
	@Transactional
	public AjaxResult insertWorkPlace(
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="check_date", required=false) String check_date,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(check_date+ " 00:00:00");
		
		BundleHead bh = new BundleHead();
		bh.setTableName("workplace_th_result");
		bh.setChar1(title);
		bh.setDate1(checkDate);
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		Integer bhId = bh.getId();
		
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		List<Map<String, Object>> itemList = (List<Map<String, Object>>) items.get(0).get("tabList");
		
		for(int i = 0; i < itemList.size(); i++) {
			MasterResult mr = new MasterResult();
			
			mr.setMasterClass("workplace_th_result");
			mr.setMasterTableId(Integer.parseInt(itemList.get(i).get("id").toString()));
			mr.setDataDate(Date.valueOf(check_date));
			
			if (itemList.get(i).get("result1") != null) {
				mr.setChar1(itemList.get(i).get("result1").toString());
			}
			if (itemList.get(i).get("result2") != null) {
				mr.setChar2(itemList.get(i).get("result2").toString());
			}
			if (itemList.get(i).get("result3") != null) {
				mr.setChar3(itemList.get(i).get("result3").toString());
			}
			if (itemList.get(i).get("result4") != null) {
				mr.setChar4(itemList.get(i).get("result4").toString());
			}
			if (itemList.get(i).get("result5") != null) {
				mr.setChar5(itemList.get(i).get("result5").toString());
			}
			
			mr.setSourceDataPk(bhId);
			mr.setSourceTableName("bundle_head");
			mr.set_audit(user);
			this.masterResultRepository.save(mr);
		}
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		paramMap.addValue("userId", user.getId());
		
		String sql = """
				insert into devi_action("SourceDataPk", "SourceTableName", "HappenDate", "HappenPlace"
			    , "AbnormalDetail", _created, _creater_id)
			    select mr.id as src_data_pk, 'workplace_th_result' , cast(to_char(b."Date1", 'yyyy-MM-dd') as date) as happen_date 
			    	,'온습도관리' as happen_place
			        , concat('부적합 : ', m."Name") as abnormal_detail       
			    ,current_date, :userId
			    from bundle_head b
			    inner join master_result mr on b.id = mr."SourceDataPk"
			    inner join master_t m on m.id = mr."MasterTable_id" and mr."Char5" = 'X'
			    where mr."SourceDataPk" = :bhId
			    and mr.id not in (
			    select a."SourceDataPk" 
			    from devi_action a
			    inner join master_result mr on mr."SourceDataPk" = b.id
			    where a."SourceTableName" = 'workplace_th_result'
			    )
			    order by m."Code" 
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		Map<String, Object> value = new HashMap<String, Object>();
		value.put("id", bh.getId());
		
		result.data = value;
		
		return result;
	}
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveWorkPlace(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="check_date", required=false) String check_date,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(check_date+ " 00:00:00");
		
		BundleHead bh =this.bundleHeadRepository.getBundleHeadById(bhId);
		bh.setChar1(title);
		bh.setDate1(checkDate);
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		List<Map<String, Object>> itemList = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		for(int i = 0; i < itemList.size(); i++) {
			MasterResult mr = this.masterResultRepository.getMasterResultById(Integer.parseInt(itemList.get(i).get("id").toString()));
			mr.setDataDate(Date.valueOf(check_date));
			
			if (itemList.get(i).get("result1") != null) {
				mr.setChar1(itemList.get(i).get("result1").toString());
			}
			if (itemList.get(i).get("result2") != null) {
				mr.setChar2(itemList.get(i).get("result2").toString());
			}
			if (itemList.get(i).get("result3") != null) {
				mr.setChar3(itemList.get(i).get("result3").toString());
			}
			if (itemList.get(i).get("result4") != null) {
				mr.setChar4(itemList.get(i).get("result4").toString());
			}
			if (itemList.get(i).get("result5") != null) {
				mr.setChar5(itemList.get(i).get("result5").toString());
			}
			
			mr.set_audit(user);
			this.masterResultRepository.saveAndFlush(mr);
		}
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		paramMap.addValue("userId", user.getId());
		
		String sql = """
				insert into devi_action("SourceDataPk", "SourceTableName", "HappenDate", "HappenPlace"
			    , "AbnormalDetail", _created, _creater_id)
			    select mr.id as src_data_pk, 'workplace_th_result' , cast(to_char(b."Date1", 'yyyy-MM-dd') as date) as happen_date 
			    	,'온습도관리' as happen_place
			        , concat('부적합 : ', m."Name") as abnormal_detail       
			    ,current_date, :userId
			    from bundle_head b
			    inner join master_result mr on b.id = mr."SourceDataPk"
			    inner join master_t m on m.id = mr."MasterTable_id" and mr."Char5" = 'X'
			    where mr."SourceDataPk" = :bhId
			    and mr.id not in (
			    select a."SourceDataPk" 
			    from devi_action a
			    inner join master_result mr on mr."SourceDataPk" = b.id
			    where a."SourceTableName" = 'workplace_th_result'
			    )
			    order by m."Code" 
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
			    delete from devi_action
                where "SourceDataPk" not in (
	                select a.id
	                from master_result a
	                left join  devi_action b on b."SourceDataPk" = a.id
	                where 1=1
	                and "Char5" = 'X'
                )
                and "SourceTableName" = 'workplace_th_result'
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
		Map<String, Object> value = new HashMap<String, Object>();
		value.put("id", bhId);
		
		result.data = value;
		
		return result;
	}
	
	@PostMapping("/mst_delete")
	public AjaxResult mstDelete(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		this.workPlaceListService.mstDelete(bhId);
		
		result.success = true;
		
		return result;
	}
}
