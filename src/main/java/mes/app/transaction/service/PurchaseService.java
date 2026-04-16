package mes.app.transaction.service;


import mes.app.util.UtilClass;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PurchaseService {


    @Autowired
    SqlRunner sqlRunner;


    public List<Map<String, Object>> getList(Map<String, Object> parameter){
        MapSqlParameterSource param = new MapSqlParameterSource();

        String spjangcd = UtilClass.getStringSafe(parameter.get("spjangcd"));
        String searchfrdate = UtilClass.getStringSafe(parameter.get("searchfrdate"));
        String searchtodate = UtilClass.getStringSafe(parameter.get("searchtodate"));
        Integer cltcd = UtilClass.parseInteger(parameter.get("cltcd"));
        //String taxtype = UtilClass.getStringSafe(parameter.get("taxtype"));
        String misgubun = UtilClass.getStringSafe(parameter.get("misgubun"));

        param.addValue("spjangcd", spjangcd);
        param.addValue("searchfrdate", searchfrdate);
        param.addValue("searchtodate", searchtodate);
        param.addValue("cltcd", cltcd);
       // param.addValue("taxtype", taxtype);
        param.addValue("misgubun", misgubun);

        String sql = """
                select
                to_char(to_date(a.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') as misdate
                ,b.spjangcd
                ,s."Value" as misgubun
                ,c."Code" as companyCode
                ,c."Name" as companyName
                ,a.itemnm
                ,a.spec
                ,COALESCE(a.supplycost, 0) AS supplycost
                ,COALESCE(a.taxtotal, 0) AS taxtotal
                ,(COALESCE(a.supplycost, 0) + COALESCE(a.taxtotal, 0)) as totalamt
                from tb_invoicedetail a
                left join tb_invoicement b
                on a.misdate = b.misdate
                and a.misnum = b.misnum
                left join company c on b.cltcd = c.id
                left join sys_code s on s."Code" = b.misgubun
                where b.spjangcd = :spjangcd
                and a.misdate between :searchfrdate and :searchtodate
                """;

        if(cltcd != null){
            sql += """
                    and b.cltcd = :cltcd
                    """;
        }

        /*if(taxtype != null && !taxtype.isEmpty()){
            sql += """
                    and b.taxtype = :taxtype
                    """;
        }*/

        if(misgubun != null && !misgubun.isEmpty()){
            sql += """
                    and b.misgubun = :misgubun
                    """;
        }

        sql += """
                order by a.misdate, b.misgubun desc;
                """;

        List<Map<String, Object>> rows = sqlRunner.getRows(sql, param);
        return rows;
    }

    public List<Map<String, Object>> getList2(Map<String, Object> parameter){
        MapSqlParameterSource param = new MapSqlParameterSource();

        String spjangcd = UtilClass.getStringSafe(parameter.get("spjangcd"));
        String searchfrdate = UtilClass.getStringSafe(parameter.get("searchfrdate"));
        String searchtodate = UtilClass.getStringSafe(parameter.get("searchtodate"));
        Integer cltcd = UtilClass.parseInteger(parameter.get("cltcd"));
        //String taxtype = UtilClass.getStringSafe(parameter.get("taxtype"));
        String misgubun = UtilClass.getStringSafe(parameter.get("misgubun"));

        param.addValue("spjangcd", spjangcd);
        param.addValue("searchfrdate", searchfrdate);
        param.addValue("searchtodate", searchtodate);
        param.addValue("cltcd", cltcd);
       // param.addValue("taxtype", taxtype);
        param.addValue("misgubun", misgubun);

        String sql = """
                select c."BusinessNumber" as saupnum
                ,count(c."BusinessNumber") as cnt
                ,c."Name" as "clientName"
                ,SUM(supplycost) as supplycost
                ,SUM(taxtotal) as taxtotal
                from tb_invoicement a
                left join company c on c.id = a.cltcd
                where a.spjangcd = :spjangcd
                and misdate between :searchfrdate and :searchtodate
                """;

        if(cltcd != null){
            sql += """
                    and cltcd = :cltcd
                    """;
        }

        /*if(taxtype != null && !taxtype.isEmpty()){
            sql += """
                    and taxtype = :taxtype
                    """;
        }*/

        if(misgubun != null && !misgubun.isEmpty()){
            sql += """
                    and misgubun = :misgubun
                    """;
        }

        sql += """
                group by c."Name", c."BusinessNumber"
                order by saupnum;;
                """;

        List<Map<String, Object>> rows = sqlRunner.getRows(sql, param);
        return rows;
    }

}
