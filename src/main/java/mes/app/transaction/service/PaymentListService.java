package mes.app.transaction.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class PaymentListService {
    @Autowired
    SqlRunner sqlRunner;

    // 지급현황 리스트 조회
    public List<Map<String, Object>> getPaymentList(String depositType, Timestamp start, Timestamp end, String company, String txtDescription, String AccountName, String txtEumnum, String spjangcd) {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("start", start);
        paramMap.addValue("end", end);
        paramMap.addValue("company", company);
        paramMap.addValue("txtDescription", txtDescription);
        paramMap.addValue("spjangcd", spjangcd);
        String sql = """
        select
           tb.ioid,
           TO_CHAR(TO_DATE(tb.trdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS trdate,
           tb.accout ,
           c."Name" as "CompanyName" ,
           tb.iotype ,
           sc."Value" as deposit_type,
           sc."Code" as deposit_code,
           tb.banknm,
           tb.accnum ,
           tt.tradenm ,
           tb.remark1,
           --rb.REMARK1 || ' ' || b.REMARK2 || ' ' || b.REMARK3 || ' ' || b.REMARK4 AS remark,
           tb.eumnum,
            CASE 
             WHEN LENGTH(TRIM(tb.eumtodt)) = 8 THEN TO_CHAR(TO_DATE(tb.eumtodt, 'YYYYMMDD'), 'YYYY-MM-DD')
             ELSE NULL
           END AS eumtodt,
           tb.memo
           from tb_banktransit tb
           left join company c on c.id = tb.cltcd  and tb.spjangcd =  c.spjangcd 
           left join  sys_code sc on sc."Code" = tb.iotype and "CodeType" ='deposit_type'
           left join tb_trade tt on tb.trid = tt.trid and tb.spjangcd = tt.spjangcd
           WHERE tb.ioflag = '1'
           -- and tb.spjangcd =:spjangcd
           AND TO_DATE(tb.trdate, 'YYYYMMDD') BETWEEN :start AND :end
           and tb.spjangcd = :spjangcd
        """;
        if (depositType != null && !depositType.isEmpty()) {
            sql += " AND tb.iotype = :depositType ";
            paramMap.addValue("depositType",  depositType );
        }

        if (company != null && !company.isEmpty()) {
            sql += " AND tb.cltcd = :company ";
            paramMap.addValue("company", Integer.parseInt(company));
        }

        if (txtDescription != null && !txtDescription.isEmpty()) {
            sql += " AND tb.remark1 ILIKE :txtDescription ";
            paramMap.addValue("txtDescription", "%" + txtDescription + "%");
        }
        if (AccountName != null && !AccountName.isEmpty()) {
            sql += " AND tb.accid = :AccountName ";
            paramMap.addValue("AccountName", Integer.parseInt(AccountName));
        }
        if (txtEumnum != null && !txtEumnum.isEmpty()) {
            sql += " AND tb.eumnum ILIKE :txtEumnum ";
            paramMap.addValue("txtEumnum", "%" + txtEumnum + "%");
        }

        sql +="""
        ORDER BY tb.trdate ASC
        """;
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
//        log.info("지급현황 read SQL: {}", sql);
//        log.info("SQL Parameters: {}", paramMap.getValues());
        return items;
    }

}
