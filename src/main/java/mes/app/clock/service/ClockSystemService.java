package mes.app.clock.service;

import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ClockSystemService {

    @Autowired
    SqlRunner sqlRunner;

    public List<Map<String, Object>> getSystemList(String spjangcd) {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();

        paramMap.addValue("spjangcd", spjangcd);

        String sql = """
            SELECT
            t.workcd
           ,t.yearflag
           ,t.worknm
           ,t.remark
           ,t.usenum
            FROM tb_pb210 t
            WHERE t.spjangcd = :spjangcd
        """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        return items;
    }


    public List<Map<String, Object>> getSystemtimeList(String spjangcd) {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();

        paramMap.addValue("spjangcd", spjangcd);

        String sql = """
            SELECT
            t.flag
           ,t.sttime
           ,t.endtime
           ,t.ovsttime
           ,t.ovedtime
           ,t.ngsttime
           ,t.ngedtime
            FROM tb_pbcont t
            WHERE t.spjangcd = :spjangcd
        """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        return items;
    }


    public Map<String, Object> getSystemDetail(String workcd,String spjangcd){

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("workcd", workcd);
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
            SELECT
            t.workcd
           ,t.yearflag
           ,t.worknm
           ,t.remark
           ,t.usenum
            FROM tb_pb210 t
            WHERE t.spjangcd = :spjangcd
            AND t.workcd = :workcd
        """;

        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
    }













}
