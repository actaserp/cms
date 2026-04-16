package mes.app.popup;

import lombok.extern.slf4j.Slf4j;
import mes.app.util.UtilClass;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class PopupService {

    @Autowired
    SqlRunner sqlRunner;

    public List<Map<String, Object>> getCltCombineList(String spjangcd, String item, String item2){
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);

        String sql = """
			WITH A AS (	select id as id
			, '0' as cltflag
			, '업체' as cltflagnm
            , "Code" as item
            , "Name" as item2
            from company
            WHERE ("CompanyType" = 'sale'
            OR "CompanyType" = 'sale-purchase')
            AND ("relyn" = '0' OR "relyn" IS NULL)
            and spjangcd = :spjangcd
            union all
            select a.id
				      , '1' as cltflag
				      , '직원정보' as cltflagnm
				      ,a."Code" as item
				      ,a."Name" as item2
				      from person a
				      left join depart d on d.id = a."Depart_id"
				      where rtflag = '0'
				      and a.spjangcd = :spjangcd
			union all
			select accid as id
				,'2' as cltflag
				, '은행계좌' as cltflagnm
				,accname as item
				,accnum as item2
				from
				tb_account a
				left join tb_xbank b on b.bankid = a.bankid
				where a.spjangcd = :spjangcd
			union all
			select 		id as id
				        ,'3' as cltflag
				        , '카드사' as cltflagnm
				        ,cardco as item
				        ,cardnum as item2
				from TB_IZ010
				where useyn = '1'
				and spjangcd = :spjangcd
				)
			SELECT * FROM A
			where 1=1
			""";

        List<Map<String, Object>> rows = sqlRunner.getRows(sql, param);

        try {
            UtilClass.decryptEachItem(rows, "item2", 0);
        } catch (IOException e) {
            log.error("복화화 중 오류발생 : {},  발생위치 : {}", e.getMessage(), this.getClass().getSimpleName());

        }

        if(item != null && !item.isEmpty()){
            rows = rows.stream()
                    .filter(t -> {
                        Object val = t.get("item");
                        return val != null && val.toString().contains(item);
                    })
                    .collect(Collectors.toList());
        }

        if(item2 != null && !item2.isEmpty()){
            rows = rows.stream()
                    .filter(t -> {
                        Object val = t.get("item2");
                        return val != null && val.toString().contains(item2);
                    }).collect(Collectors.toList());
        }

        return rows;
    }
}
