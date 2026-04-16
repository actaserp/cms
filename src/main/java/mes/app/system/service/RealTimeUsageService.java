package mes.app.system.service;

import mes.domain.services.SqlRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class RealTimeUsageService {

    private static final Logger logger = LoggerFactory.getLogger(RealTimeUsageService.class);

    @Autowired
    SqlRunner sqlRunner;

    public List<Map<String, Object>> getUsageList(String spjangcd){
        long startTime = System.currentTimeMillis();

        MapSqlParameterSource param = new MapSqlParameterSource();

        param.addValue("spjangcd", spjangcd);

        String sql = """
                select
                TO_CHAR(CURRENT_DATE, 'YYYY-MM') AS stat_day
                ,b.name
                ,b.price  --단가
                ,b.api_call_limit as api_call_limit --기본제공 api
                ,b.remark --비고
                ,b.extra_api_unit_price
                from tb_xa012 a
                left join bill_plans b on a.bill_plans_id = b.id
                where
                spjangcd = :spjangcd
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

        long endTime = System.currentTimeMillis();
        logger.info("RealTimeUsageService.getUsageList [spjangcd: {}] execution time: {} ms", spjangcd, (endTime - startTime));

        return items;
    }

    public List<Map<String, Object>> getApiUsageHistory(String spjangcd){

        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);

        String sql = """
                select
                TO_CHAR(stat_day, 'YYYY-MM') as stat_day,
                bill_plan_name as name,
                price,
                sum(total_count) as total_count,
                api_call_limit,
                '서비스 월 요금' as remark,
                extra_api_unit_price
                from api_log_entry
                where spjangcd = :spjangcd
                GROUP BY 1, bill_plan_name, price, api_call_limit, remark, extra_api_unit_price
                order by TO_CHAR(stat_day, 'YYYY-MM') desc
                """;
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

        return items;
    }
}
