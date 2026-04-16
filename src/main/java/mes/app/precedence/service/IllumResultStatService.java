package mes.app.precedence.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class IllumResultStatService {

	@Autowired
	SqlRunner sqlRunner;

	// 결재현황 조회
	public List<Map<String, Object>> getIllumResultApprStat(String data_year, String data_month, String appr_state) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("data_year", data_year);   
        dicParam.addValue("data_month", data_month);
        dicParam.addValue("appr_state", appr_state);
        
        String sql = """
        		select b.id, b."Char1" as "Title", b."Char3" as "Month", b."Text1" as "Floor"
                , coalesce(r."State", 'write') as "State"
                , coalesce(r."StateName", '작성') as "StateName"
                , r."LineName"
                , r."LineNameState"
                , to_char(b."Date1", 'YYYY-MM-DD') as "DataDate"
                , coalesce(r."SearchYN", 'Y') as "SearchYN"
                , coalesce(r."EditYN", 'Y') as "EditYN"
                , coalesce(r."DeleteYN", 'Y') as "DeleteYN"
                from bundle_head b 
                left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                where b."TableName" = 'illum_zone'
                and b."Char2" = :data_year
        		""";
        
        if (!data_month.isEmpty() && data_month != null) {
        	sql += " and b.\"Char3\" = :data_month ";
        }
        
        if (!appr_state.isEmpty() && appr_state != null) {
			if(appr_state.equals("write")) {
				sql += " and r.\"State\" is null ";
			}
			else {
				sql += " and r.\"State\" = :appr_state ";
			}
		}
		sql += " order by b.\"Date1\" desc, b.id desc ";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}

}
