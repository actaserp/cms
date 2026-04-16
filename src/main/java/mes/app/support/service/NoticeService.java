package mes.app.support.service;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.DateUtil;
import mes.domain.services.SqlRunner;

@Service
public class NoticeService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 공지사항 조회
	public List<Map<String, Object>> getBoardList(String board_group, String keyword, String srchStartDt, String srchEndDt) {
		
		String today = DateUtil.getTodayString();
		String tenantId = TenantContext.get();

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("board_group", board_group);
		paramMap.addValue("srchStartDt", Timestamp.valueOf(srchStartDt));		
		paramMap.addValue("srchEndDt", Timestamp.valueOf(srchEndDt));
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("today", Date.valueOf(today));
		paramMap.addValue("spjangcd", tenantId);
        
        String sql = """
        		with A as (
                    select id, "Title" as title
	                , to_char("WriteDateTime", 'yyyy-mm-dd hh24:mi:ss') as write_date_time
	                from board
	                where "BoardGroup" = :board_group
                    and "NoticeYN" = 'Y'
	                and "NoticeEndDate" >= :today
	                and spjangcd = :spjangcd
                ), B as (
                    select B.id, B."Title" as title
                    , to_char(B."WriteDateTime", 'yyyy-mm-dd hh24:mi:ss') as write_date_time
                    from board B 
                    left join A on A.id = B.id
                    where B."BoardGroup" = :board_group
                    and B."WriteDateTime" between :srchStartDt and :srchEndDt
                    and A.id is null
                    and B.spjangcd = :spjangcd
        		     """;
        
        if (StringUtils.isEmpty(keyword) == false) {
        	sql += """
        			and ( B."Title" like concat('%%', :keyword, '%%') 
                        or B."Content" like concat('%%', :keyword, '%%')
                        )
        			""";
        }
        
        sql += """
        		)
            select 1 as data_group, id, title, write_date_time
            from A 
            union all 
            select 2 as data_group, id, title, write_date_time
            from B 
            order by data_group, write_date_time desc
        		""";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	// 공지사항 상세조회
	public Map<String, Object> getBoardDetail(Integer id){
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("id", id);
        
        String sql = """
        		select id
	            , "Title" title 
	            , "NoticeYN" notice_yn
	            , "NoticeEndDate" notice_end_date
	            , "Content" as content
            from board b 
            where id = :id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
        
        return item;
	}
}
