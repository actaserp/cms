package mes.app.transaction.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ProjectRegistrationServicr {
  @Autowired
  SqlRunner sqlRunner;


  public List<Map<String, Object>> getProjectList(String srchStartDt, String srchEndDt, String spjangcd,
                                                  String cboCompany, String txtDescription) {
    MapSqlParameterSource dicParam = new MapSqlParameterSource();

    dicParam.addValue("srchStartDt", srchStartDt);
    dicParam.addValue("srchEndDt", srchEndDt);
    dicParam.addValue("spjangcd", spjangcd);
    dicParam.addValue("cboCompany", cboCompany);
    dicParam.addValue("txtDescription", txtDescription);

    String sql = """
        SELECT * 
        FROM tb_da003 td 
        WHERE td.spjangcd = :spjangcd
          AND td.contdate BETWEEN :srchStartDt AND :srchEndDt
    """;
    if (cboCompany != null && !cboCompany.isEmpty()) {
      sql += "  AND td.balcltcd = :cboCompany ";
      dicParam.addValue("cboCompany", Integer.parseInt(cboCompany));
    }
    if (txtDescription != null && !txtDescription.isEmpty()) {
      sql += " AND td.projnm LIKE :txtDescription ";
      dicParam.addValue("txtDescription", "%" + txtDescription + "%");
    }

//    log.info("프로젝트 관리 read SQL: {}", sql);
//    log.info("SQL Parameters: {}", dicParam.getValues());
    List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);

    return itmes;
  }
}
