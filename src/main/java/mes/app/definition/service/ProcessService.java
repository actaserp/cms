package mes.app.definition.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mes.domain.repository.ProcessRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

import javax.transaction.Transactional;

@Service
public class ProcessService {

	@Autowired
	ProcessRepository processRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	
	//공정 목록 조회
	public List<Map<String, Object>> getProcessList(String processName, String spjangcd){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("process_name", processName);
		dicParam.addValue("spjangcd", spjangcd);
        
        String sql = """
			select p.id
            , p."Code" as process_code
            , p."Name" as process_name
            , p."ProcessType" as process_type
            , to_char(p."_created" ,'yyyy-mm-dd hh24:mi') as created
            , f."Name" as factory_name
            , p."Factory_id" as factory_id
            , p."Description" as description
            from process p 
            left join factory f on p."Factory_id"=f.id 
            where 1=1
            AND p.spjangcd = :spjangcd
            """;
        if (StringUtils.isEmpty(processName)==false) sql +="and upper(p.\"Name\") like concat('%%',upper(:process_name),'%%')";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
	// 공정 상세정보 조회
	public Map<String, Object> getProcessDetail(int processId){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("process_id", processId);
        
        String sql = """
			select p.id
		    , p."Code" as process_code
		    , p."Name" as process_name
		    , p."ProcessType" as process_type
		    , to_char(p."_created" ,'yyyy-mm-dd hh24:mi') as created
		    , f."Name" as factory_name
		    , p."Factory_id" as factory_id
		    , p."Description" as description
		    from process p 
		    left join factory f on p."Factory_id"=f.id 
		    where 1=1
		    and p.id = :process_id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        
        return item;
	}
	
	// 공정별 부적합 유형
	public List<Map<String, Object>> getProcDefectTypeList(int processId, String spjangcd){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("process_id", processId);
		dicParam.addValue("spjangcd", spjangcd);
        
        String sql = """
			select pd.id, dt.id as defect_type_id
	        , dt."Code" as defect_type_code
	        , dt."Name" as defect_type_name
	        from proc_defect_type pd 
	        inner join defect_type dt on dt.id = pd."DefectType_id"
	        where pd."Process_id" = :process_id
	        AND pd.spjangcd = :spjangcd
	        order by 2
		    """;
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
	// 공정별 비가동 유형
	public List<Map<String, Object>> getProcStopCauseList(int processId, String spjangcd){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("process_id", processId);
		dicParam.addValue("spjangcd", spjangcd);

        String sql = """
			select ps.id, sc.id as stop_cause_id
	        , sc."StopCauseName" as stop_cause_name
	        from proc_stop_cause ps
	        inner join stop_cause sc on sc.id = ps."StopCause_id"
	        where ps."Process_id" = :process_id
	        AND pd.spjangcd = :spjangcd
	        order by 2
		    """;
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
	// 공정정보 저장
	public int saveProcess(MultiValueMap<String, Object> data) {
		Integer id = CommonUtil.tryIntNull(data.getFirst("id"));
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("id", id);
		dicParam.addValue("code", CommonUtil.tryString(data.getFirst("process_code")));
		dicParam.addValue("name", CommonUtil.tryString(data.getFirst("process_name")));
		dicParam.addValue("processType", CommonUtil.tryString(data.getFirst("process_type")));
		dicParam.addValue("description", CommonUtil.tryString(data.getFirst("description")));
		dicParam.addValue("factoryId", CommonUtil.tryIntNull(data.getFirst("factory_id")));
		dicParam.addValue("user_id", CommonUtil.tryIntNull(data.getFirst("user_id")));
		
		String sql = "";
		
		if (id==null) {
			sql = """
					INSERT INTO process
					("_created","_creater_id", "Code", "Name"
					,"ProcessType", "Description", "Factory_id")
					VALUES
					(now(), :user_id, :code, :name
					, :processType, :description, :factoryId)
					""";
		} else {
			sql = """
					UPDATE process SET "_modified" = now(),"_modifier_id" = :user_id
					, "Code" = :code, "Name" = :name
					, "ProcessType" = :processType
					, "Description" = :description
					, "Factory_id" = :factoryId
					WHERE id = :id
					""";
		}
		return this.sqlRunner.execute(sql, dicParam);
	}

	public List<String> getSuggestions(String query, String field) {

		return switch (field) {
			case "process_type" -> processRepository.findProcessTypeByQuery(query);

			// 다른 필드에 대한 처리
			default -> new ArrayList<>();
		};
	}



}
