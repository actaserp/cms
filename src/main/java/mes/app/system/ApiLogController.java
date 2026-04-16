package mes.app.system;


import lombok.RequiredArgsConstructor;
import mes.domain.model.AjaxResult;
import mes.domain.services.SqlRunner;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@RestController
@RequestMapping("/api/system/api-log")
@RequiredArgsConstructor
public class ApiLogController {

    private final SqlRunner sqlRunner;


    @GetMapping("/read")
    public AjaxResult getList(@RequestParam String srchStartDt,
                              @RequestParam String srchEndDt,
                              @RequestParam String keyword
                              ){

        MapSqlParameterSource param = new MapSqlParameterSource();


        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        LocalDateTime st = LocalDateTime.parse(srchStartDt + " 00:00:00", formatter);
        LocalDateTime ed = LocalDateTime.parse(srchEndDt + " 23:59:59", formatter);


        param.addValue("st", st);
        param.addValue("en" , ed);
        param.addValue("endpoint" , "%" + keyword + "%");


        String sql = """
                    select * from api_log_entry
                    where "log_timestamp" between :st and :en
                """;

        if(!keyword.isEmpty())

        {
            sql += """
                and endpoint like :endpoint
                """;
        }

        sql += """
                order by avg_call_cnt desc
                """;

        AjaxResult result = new AjaxResult();

        result.data = sqlRunner.getRows(sql, param);

        return result;
    }
}
