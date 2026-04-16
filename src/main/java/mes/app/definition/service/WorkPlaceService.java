package mes.app.definition.service;

import mes.app.common.TenantContext;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class WorkPlaceService {
    @Autowired
    SqlRunner sqlRunner;

    // 세무서 이름 조회
    public String getTaxnm(String taxcd){

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
				String tenantId = TenantContext.get();
				dicParam.addValue("spjangcd", tenantId);
        dicParam.addValue("taxcd", taxcd);
        String sql = """
			select taxnm
			from tb_xatax
			where taxcd = :taxcd
		    """;

        Map<String, Object> tax = this.sqlRunner.getRow(sql, dicParam);

        return tax.get("taxnm").toString();
    }
    // 세무서 리스트 조회
    public List<Map<String, Object>> getPopupList(String taxcd, String taxnm, String taxjiyuk){

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        String sql = """
			select *
			from tb_xatax
			where 1=1
		    """;
        if(taxcd != null && !taxcd.isEmpty()){
            dicParam.addValue("taxcd", "%" + taxcd + "%");
            sql += " and taxcd LIKE :taxcd";
        }
        if(taxnm != null && !taxnm.isEmpty()){
            dicParam.addValue("taxnm", "%" + taxnm + "%");
            sql += " and taxnm LIKE :taxnm";
        }
        if(taxjiyuk != null && !taxjiyuk.isEmpty()){
            dicParam.addValue("taxjiyuk", "%"+ taxjiyuk +"%");
            sql += " and taxjiyuk LIKE :taxjiyuk";
        }

        List<Map<String, Object>> popupList = this.sqlRunner.getRows(sql, dicParam);

        return popupList;
    }
}
