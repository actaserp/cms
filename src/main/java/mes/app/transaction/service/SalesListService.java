package mes.app.transaction.service;


import mes.app.util.UtilClass;
import mes.domain.enums.IssueState;
import mes.domain.services.SqlRunner;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

@Service
public class SalesListService {

    @Autowired
    SqlRunner sqlRunner;

    public List<Map<String, Object>> getList(Map<String, Object> parameter){
        MapSqlParameterSource param = new MapSqlParameterSource();

        String spjangcd = UtilClass.getStringSafe(parameter.get("spjangcd"));
        String searchfrdate = UtilClass.getStringSafe(parameter.get("searchfrdate"));
        String searchtodate = UtilClass.getStringSafe(parameter.get("searchtodate"));
        Integer cltcd = UtilClass.parseInteger(parameter.get("cltcd"));
        String taxtype = UtilClass.getStringSafe(parameter.get("taxtype"));
        String misgubun = UtilClass.getStringSafe(parameter.get("misgubun"));

        param.addValue("spjangcd", spjangcd);
        param.addValue("searchfrdate", searchfrdate);
        param.addValue("searchtodate", searchtodate);
        param.addValue("cltcd", cltcd);
        param.addValue("taxtype", taxtype);
        param.addValue("misgubun", misgubun);

        String sql = """
                select
                to_char(to_date(a.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') as misdate
                ,b.spjangcd
                ,s."Value" as misgubun
                ,c."Code" as companyCode
                ,c."Name" as companyName
                ,b.iveremail
                ,a.itemnm
                ,a.spec
                ,COALESCE(a.supplycost, 0) AS supplycost
                ,COALESCE(a.taxtotal, 0) AS taxtotal
                ,(COALESCE(a.supplycost, 0) + COALESCE(a.taxtotal, 0)) as totalamt
                ,b.statecode
                from tb_salesdetail a
                left join tb_salesment b
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

        if(taxtype != null && !taxtype.isEmpty()){
            sql += """
                    and b.taxtype = :taxtype
                    """;
        }

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
        String taxtype = UtilClass.getStringSafe(parameter.get("taxtype"));
        String misgubun = UtilClass.getStringSafe(parameter.get("misgubun"));

        param.addValue("spjangcd", spjangcd);
        param.addValue("searchfrdate", searchfrdate);
        param.addValue("searchtodate", searchtodate);
        param.addValue("cltcd", cltcd);
        param.addValue("taxtype", taxtype);
        param.addValue("misgubun", misgubun);

        String sql = """
                select ivercorpnum
                ,count(ivercorpnum) as cnt
                ,ivercorpnm
                ,SUM(supplycost) as supplycost
                ,SUM(taxtotal) as taxtotal
                from tb_salesment
                where spjangcd = :spjangcd
                and misdate between :searchfrdate and :searchtodate
                """;

        if(cltcd != null){
            sql += """
                    and cltcd = :cltcd
                    """;
        }

        if(taxtype != null && !taxtype.isEmpty()){
            sql += """
                    and taxtype = :taxtype
                    """;
        }

        if(misgubun != null && !misgubun.isEmpty()){
            sql += """
                    and misgubun = :misgubun
                    """;
        }

        sql += """
                group by ivercorpnum, ivercorpnm
                order by ivercorpnum;
                """;

        List<Map<String, Object>> rows = sqlRunner.getRows(sql, param);
        return rows;
    }

    /*public Map<String, Object> StatisticsCalculator(List<Map<String, Object>> list){

        Map<String, Object> bucket = new HashMap<>();

        int SaupCltSum = 0;
        int SaupCntSum = 0;
        int SaupSupplySum = 0;
        int SaupTaxSum = 0;

        int PersonCltSum = 0;
        int PersonCntSum = 0;
        int PersonSupplySum = 0;
        int PersonTaxSum = 0;


        for(Map<String, Object> item : list){

            Object cnt = item.get("cnt");
            Object supply = item.get("supplycost");
            Object tax = item.get("taxtotal");
            Object corpnum = item.get("ivercorpnum");


            int parsedCnt = cnt != null ? UtilClass.parseInteger(cnt) : 0;
            int parsedSupply = supply != null ? UtilClass.parseInteger(supply) : 0;
            int parsedTax = tax != null ? UtilClass.parseInteger(tax) : 0;
            String parsedCorpNum = corpnum != null ? corpnum.toString() : "";

            //주민번호 일 경우 (개인)
            if(parsedCorpNum.length() > 11){
                PersonCltSum++;
                PersonCntSum += parsedCnt;
                PersonSupplySum += parsedSupply;
                PersonTaxSum += parsedTax;
            }else{
                SaupCltSum++;
                SaupCntSum += parsedCnt;
                SaupSupplySum += parsedSupply;
                SaupTaxSum += parsedTax;
            }

        }

        bucket.put("cltCnt", SaupCltSum + PersonCltSum);
        bucket.put("cnt", SaupCntSum + PersonCntSum);
        bucket.put("supplySum", SaupSupplySum + PersonSupplySum);
        bucket.put("taxSum", SaupTaxSum + PersonTaxSum);

        bucket.put("SaupCltSum", SaupCltSum);
        bucket.put("SaupCntSum", SaupCntSum);
        bucket.put("SaupSupplySum", SaupSupplySum);
        bucket.put("SaupTaxSum", SaupTaxSum);

        bucket.put("PersonCltSum", PersonCltSum);
        bucket.put("PersonCntSum", PersonCntSum);
        bucket.put("PersonSupplySum", PersonSupplySum);
        bucket.put("PersonTaxSum", PersonTaxSum);

        return bucket;
    }*/

}
