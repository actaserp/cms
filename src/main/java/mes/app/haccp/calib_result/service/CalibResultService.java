package mes.app.haccp.calib_result.service;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class CalibResultService {

	@Autowired
	SqlRunner sqlRunner;

	// 검교정결과 상세조회
	public Map<String, Object> getCalibResultDetailList(Integer id, String table_name) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();                  
        dicParam.addValue("id", id);
        dicParam.addValue("tableName", table_name);
        
        String sql = """
			with F as (
	                select f."DataPk" as data_pk
                    , row_number() over (partition by f."DataPk", f."AttachName" order by f.id desc) as g_idx
	                , f."AttachName" as attach_name, f.id as file_id, f."FileName" as file_name
	                from attach_file f 
	                where f."TableName" = :tableName
	                and f."AttachName" in ( 'standard_photo', 'calib_photo' )
	                and f."DataPk" = :id
                ), F2 as (
	                select F.data_pk
	                , min(case when F.attach_name = 'standard_photo' then F.file_id end) as standard_file_id
	                , min(case when F.attach_name = 'standard_photo' then F.file_name end) as standard_file_name
	                , min(case when F.attach_name = 'calib_photo' then F.file_id end) as calib_file_id
	                , min(case when F.attach_name = 'calib_photo' then F.file_name end) as calib_file_name
	                from  F 
	                where F.g_idx = 1 
	                group by F.data_pk
                )	
            select cr.id, c.id as "CalibInstrument_id", c."Name" as "CalibInstName", c."CalibInstClass"
	        , cr."CalibDate", cr."CalibInstitution" 
	        , cr."Difference", cr."CalibJudge", cr."Description" 
            , F2.standard_file_id, F2.standard_file_name, F2.calib_file_id, F2.calib_file_name
	        from calib_result cr
	        inner join calib_inst c on c.id = cr."CalibInstrument_id"
            left join F2 on F2.data_pk = cr.id
	        where cr.id = :id
        	""";

        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
	}
	
	public void updateCalibInst (Integer calib_inst_id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("calib_inst_id", calib_inst_id);
	    
		String sql = """
				with A as (
		            select cr."CalibInstrument_id" as inst_id, max("CalibDate") as calib_date
		            from calib_result cr
		            where cr."CalibInstrument_id" = :calib_inst_id
		            group by cr."CalibInstrument_id"
	            )
	            update calib_inst
	            set "SelfCalibDate" = A.calib_date
	            , "NextCalibDate" = case when calib_inst."CycleBase" = 'Y' then dateadd(year, calib_inst.CycleNumber, A.calib_date)
							        when calib_inst."CycleBase" = 'M' then dateadd(month, calib_inst.CycleNumber, A.calib_date) end
	            from A
	            where calib_inst.id = A.inst_id
				""";
		
        this.sqlRunner.execute(sql, paramMap);    
	}
	
}
