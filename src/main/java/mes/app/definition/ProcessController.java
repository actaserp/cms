package mes.app.definition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.domain.entity.User;
import mes.app.definition.service.ProcessService;
import mes.domain.entity.Process;
import mes.domain.model.AjaxResult;
import mes.domain.repository.ProcessRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/definition/process")
public class ProcessController {
	
	@Autowired
	ProcessRepository processRepository;
	
	@Autowired
	private ProcessService processService;
	
	@Autowired
	SqlRunner sqlRunner;
	
	// 공정 목록 조회
	@GetMapping("/read")
	public AjaxResult getProcessList(
			@RequestParam("process_name") String processName,
			@RequestParam(value ="spjangcd") String spjangcd,
    		HttpServletRequest request) {
       
        List<Map<String, Object>> items = this.processService.getProcessList(processName,spjangcd);
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	// 공정 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getProcessDetail(
			@RequestParam("process_id") int processId, 
    		HttpServletRequest request) {
        Map<String, Object> item = this.processService.getProcessDetail(processId);      
               		
        AjaxResult result = new AjaxResult();
        result.data = item;        				
        
		return result;
	}
	
	// 공정정보 저장
	@PostMapping("/save")
	public AjaxResult saveProcess(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("process_code") String code,
			@RequestParam("process_name") String name,
			@RequestParam(value="process_type", required=false) String processType,
			@RequestParam(value="description", required=false) String description,
			@RequestParam("factory_id") Integer factoryId,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		Process process =null;
		boolean code_chk = this.processRepository.findByCode(code).isEmpty();
		
		boolean name_chk = this.processRepository.findByName(name).isEmpty();
		
		// 코드 중복 체크
		if (id==null) {
			if (code_chk == false) {
				result.success = false;
				result.message="중복된 코드가 존재합니다.";
				return result;
			}
			process = new Process();
		} else {
			process = this.processRepository.getProcessById(id);
			if (code.equals(process.getCode())==false && code_chk == false) {
				result.success = false;
				result.message="중복된 코드가 존재합니다.";
				return result;
			}
		}
		
		// 이름 중복 체크
		if (id==null) {
			if (name_chk == false) {
				result.success = false;
				result.message="중복된 이름이 존재합니다.";
				return result;
			}
			process = new Process();
		} else {
			process = this.processRepository.getProcessById(id);
			if (name.equals(process.getName())==false && name_chk == false) {
				result.success = false;
				result.message="중복된 이름이 존재합니다.";
				return result;
			}
		}
		process.setCode(code);
		process.setName(name);
		process.setProcessType(processType);
		process.setDescription(description);
		process.setFactory_id(factoryId);
		process.set_audit(user);
		process.setSpjangcd(spjangcd);

		
		process = this.processRepository.save(process);
	
        result.data=process;
		
		return result;
	}
	
	// 공정정보 삭제
	@PostMapping("/delete")
	public AjaxResult deleteProcess(@RequestParam("id") int id) {
        
        this.processRepository.deleteById(id);
        AjaxResult result = new AjaxResult();
        
		return result;
	}
	
	// 공정별 부적합 유형 조회
	@GetMapping("/proc_defect_type_list")
	public AjaxResult getProcDefectTypeList(
			@RequestParam("proc_pk") int processId,
			@RequestParam(value ="spjangcd") String spjangcd,
    		HttpServletRequest request) {
		List<Map<String, Object>> items = this.processService.getProcDefectTypeList(processId,spjangcd);
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}

	// 공정별 비가동 유형 조회
	@GetMapping("/proc_stop_cause_list")
	public AjaxResult getProcStopCauseList(
			@RequestParam("proc_pk") int processId,
			@RequestParam(value ="spjangcd") String spjangcd,
    		HttpServletRequest request) {
		List<Map<String, Object>> items = this.processService.getProcStopCauseList(processId,spjangcd);
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	// 공정별 부적합 유형 저장
	@PostMapping("/save_proc_defect_type")
	public AjaxResult saveProcDefectType(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		Integer proc_pk = CommonUtil.tryIntNull(Q.getFirst("proc_pk"));
	    List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
	    
	    List<String> list = new ArrayList<>();
	    data.forEach(item -> list.add(item.get("defect_type_id").toString()));
	    String defect_type_ids = String.join(",", list);
		
	    MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("proc_pk", proc_pk);
		dicParam.addValue("defect_type_ids", defect_type_ids);
		
		String sql = """
				delete from proc_defect_type 
				where "Process_id" = :proc_pk
			    """;
	    this.sqlRunner.execute(sql, dicParam);
	    
	    String sql2 = """
				with A as (
                    select unnest(string_to_array(:defect_type_ids, ','))::int as defect_type_pk
                )
                insert into proc_defect_type("Process_id", "DefectType_id",_created)
                select :proc_pk, A.defect_type_pk, now()
                from A
			    """;
	    
	    this.sqlRunner.execute(sql2, dicParam);
		return result;
	
	}
	
	// 공정별 비가동 유형 저장
	@PostMapping("/save_proc_stop_cause")
	public AjaxResult saveProcStopCause(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
	    Integer proc_pk = CommonUtil.tryIntNull(Q.getFirst("proc_pk"));
	    List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
	    
	    List<String> list = new ArrayList<>();
	    data.forEach(item -> list.add(item.get("stop_cause_id").toString()));
	    String stop_cause_ids = String.join(",", list);
	    
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("proc_pk", proc_pk);
		dicParam.addValue("stop_cause_ids", stop_cause_ids);
		
		String sql = """
				delete from proc_stop_cause 
				where "Process_id" = :proc_pk
			    """;
	    this.sqlRunner.execute(sql, dicParam);
	    
	    String sql2 = """
				with A as (
                    select unnest(string_to_array(:stop_cause_ids, ','))::int as stop_cause_pk
                )
                insert into proc_stop_cause("Process_id", "StopCause_id",_created)
                select :proc_pk, A.stop_cause_pk, now()
                from A
			    """;
	    
	    this.sqlRunner.execute(sql2, dicParam);
		return result;
	}


	@GetMapping("/autocomplete")
	public ResponseEntity<List<String>> autocomplete(@RequestParam String query, @RequestParam String field) {
		List<String> suggestions = processService.getSuggestions(query, field);
		return ResponseEntity.ok(suggestions);
	}



}
