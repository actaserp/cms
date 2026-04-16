package mes.app.definition.service;

import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class AccSubjectService {
    @Autowired
    SqlRunner sqlRunner;

    public List<Map<String, Object>> getAccList(String spjangcd, String acccd, String accnm, String useyn) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
            SELECT
                A.acccd,
                A.accnm,
                A.accprtnm,
                A.uacccd,
                A.acclv,
                A.drcr,
                A.dcpl,
                A.spyn,
                A.useyn,
                A.cacccd,
                A.etccode,
                B.accnm AS uaccde_name
            FROM tb_accsubject A
            LEFT JOIN tb_accsubject B
                ON A.uacccd = B.acccd
            WHERE A.spjangcd = :spjangcd
        """;

        if(acccd != null && !acccd.isEmpty()){
            dicParam.addValue("acccd", "%" + acccd + "%");
            sql += " AND A.acccd LIKE :acccd";
        }
        if(accnm != null && !accnm.isEmpty()){
            dicParam.addValue("accnm", "%" + accnm + "%");
            sql += " AND A.accnm LIKE :accnm";
        }
        if(useyn != null && !useyn.isEmpty()){
            dicParam.addValue("useyn", useyn);
            sql += " AND A.useyn = :useyn";
        }

        sql += " ORDER BY A.acccd";

        return this.sqlRunner.getRows(sql, dicParam);
    }



    public List<Map<String, Object>> getAccSearchitem(String code, String name,String spjangcd) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("code", code);
        dicParam.addValue("name", name);
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
                 select
                 A.acccd as acccd
                 , A.accnm as accnm
                 from tb_accsubject A
                where 
                A.spyn = '1'
                AND A.acccd like concat('%',:code,'%')
                AND A.accnm like concat('%',:name,'%')
                AND A.spjangcd = :spjangcd
                order by A.acccd
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }


    public List<Map<String, String>> getAccCodeAndAccnmAndAcclvList() {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String sql = """
                SELECT acccd, accnm, acclv, spyn
                FROM tb_accsubject
            """;
        // SQL 실행
        List<Map<String, Object>> rows = this.sqlRunner.getRows(sql, dicParam);

        List<Map<String, String>> result = rows.stream()
                .map(row -> {
                    Map<String, String> map = new HashMap<>();
                    map.put("acccd", (String) row.get("acccd"));
                    map.put("accnm", (String) row.get("accnm"));
                    map.put("acclv", String.valueOf(row.get("acclv")));
                    map.put("spyn",  (String) row.get("spyn"));
                    return map;
                })
                .toList();

        return result;
    }

    public Map<String, Object> getAccDetail(String id){
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("id", id);

        String sql = """	
             select b.acccd
	            , b.accnm
	            , b.accprtnm
	            , b.uacccd
	            , b.acclv
	            , b.drcr
	            , b.dcpl
	            , b.spyn
	            , b.useyn
	            , b.cacccd
	            , b.etccode
	            from tb_accsubject b
	            where b.acccd=:id
            """;

        return this.sqlRunner.getRow(sql, paramMap);
    }

    public List<Map<String, Object>> getAddDetail(String id) {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("id", id);

        String sql = """
            select
                m.itemcd as code,
                m.itemnm as name,
                m.essyn as required,
                m.useyn as used
            from tb_accsubject b
            left join tb_accmanage m on b.acccd = m.acccd
            where b.acccd = :id
        """;

        return this.sqlRunner.getRows(sql, paramMap);
    }





}
