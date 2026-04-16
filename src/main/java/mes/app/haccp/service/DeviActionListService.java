package mes.app.haccp.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.services.SqlRunner;

@Service
public class DeviActionListService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getDeviActionList(String keyword, String dateFrom, String dateTo) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dateFrom", dateFrom);
		paramMap.addValue("dateTo", dateTo);
		paramMap.addValue("keyword", keyword);
		
		String sql = """
				select   da."HappenDate" as happen_date
				       , da."HappenPlace" as happen_place
				       , da."AbnormalDetail" as abnormal_detail
				       , da."ActionDetail" as action_detail
				       , up2."Name" as creater_name
				       , up."Name" as action_name
				from devi_action da 
				left join user_profile up on up."User_id" = da._modifier_id
				left join user_profile up2 on up2."User_id" = da._creater_id
				where 1=1
				and da."HappenPlace" != '에어컴프레샤'
				and da."HappenDate" between cast(:dateFrom as date) and cast(:dateTo as date)
				""";
		
		if(StringUtils.hasText(keyword)) {
			sql += " and upper(da.\"HappenPlace\") like concat('%%',upper(:keyword),'%%')  ";
		}
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

}
