package mes.app.cms.service;

import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;

@Service
public class CmsBankCodeService {

    @Autowired
    SqlRunner sqlRunner;

    /** CMS 은행코드 전체 목록 */
    public List<Map<String, Object>> getBankCodeList(String bankName) {
        MapSqlParameterSource param = new MapSqlParameterSource();

        String sql = """
                SELECT id, bank_code, bank_name, use_yn
                FROM cms_bank_code
                WHERE 1 = 1
                """;

        if (StringUtils.hasText(bankName)) {
            sql += " AND bank_name LIKE '%' || :bankName || '%'";
            param.addValue("bankName", bankName);
        }

        sql += " ORDER BY bank_code";
        return sqlRunner.getRows(sql, param);
    }

    /** 저장 (신규/수정) */
    public Long saveBankCode(Long id, String bankCode, String bankName, String useYn) {
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("bankCode", bankCode);
        param.addValue("bankName", bankName);
        param.addValue("useYn", useYn != null ? useYn : "Y");

        if (id == null) {
            String sql = """
                    INSERT INTO cms_bank_code (bank_code, bank_name, use_yn)
                    VALUES (:bankCode, :bankName, :useYn)
                    RETURNING id
                    """;
            Map<String, Object> row = sqlRunner.getRow(sql, param);
            if (row == null) return null;
            return ((Number) row.get("id")).longValue();
        } else {
            param.addValue("id", id);
            String sql = """
                    UPDATE cms_bank_code
                    SET bank_code = :bankCode,
                        bank_name = :bankName,
                        use_yn    = :useYn
                    WHERE id = :id
                    """;
            return sqlRunner.execute(sql, param) > 0 ? id : null;
        }
    }

    /** 삭제 */
    public boolean deleteBankCode(Long id) {
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("id", id);
        return sqlRunner.execute("DELETE FROM cms_bank_code WHERE id = :id", param) > 0;
    }
}
