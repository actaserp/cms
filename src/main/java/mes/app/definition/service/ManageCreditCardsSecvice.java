package mes.app.definition.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.aspect.DecryptField;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static mes.Encryption.EncryptionUtil.decrypt;

@Slf4j
@Service
public class ManageCreditCardsSecvice {

  @Autowired
  SqlRunner sqlRunner;

  @DecryptField(columns = {"cardnum", "accnum"} )
  public List<Map<String, Object>> getCreditCardsList(String spjangcd, String txtcardnm, String txtcardnum) {
    MapSqlParameterSource dicParam = new MapSqlParameterSource();

    dicParam.addValue("spjangcd", spjangcd);
    dicParam.addValue("txtcardnm", txtcardnm); //카드명
    dicParam.addValue("txtcardnum", txtcardnum);  //카드번호

    String sql = """
        select *
        from tb_iz010 ti
        where ti.spjangcd = :spjangcd 
        """;
    if (txtcardnm != null && !txtcardnm.isEmpty()) {
      sql += " and ti.cardnm like :txtcardnm ";
      dicParam.addValue("txtcardnm", "%" + txtcardnm + "%");
    }
    // 쿼리 실행
    List<Map<String, Object>> rawResults = this.sqlRunner.getRows(sql, dicParam);

    // 자바단에서 복호화 후 카드번호 필터링
    if (txtcardnum != null && !txtcardnum.isEmpty()) {
      rawResults = rawResults.stream()
          .filter(item -> {
            String encrypted = String.valueOf(item.get("cardnum"));
            String decrypted;
            try {
              decrypted = decrypt(encrypted);
            } catch (Exception e) {
              throw new RuntimeException("복호화 실패", e);
            }
            return decrypted.contains(txtcardnum);
          })
          .map(item -> {
            try {
              item.put("cardnum", decrypt(item.get("cardnum").toString())); // 복호화 값 덮어쓰기
            } catch (Exception e) {
              throw new RuntimeException("복호화 실패", e);
            }
            return item;
          })
          .collect(Collectors.toList());
    }
    return rawResults;
  }

  public String findDecryptedAccountNumberByAccid(Integer accid) throws Exception {
    MapSqlParameterSource dicParam = new MapSqlParameterSource();
    dicParam.addValue("accid", accid);

    String sql = """
        SELECT accnum 
        FROM tb_account 
        WHERE accid = :accid
        """;

    List<Map<String, Object>> result = this.sqlRunner.getRows(sql, dicParam);

    if (result.isEmpty()) {
      throw new RuntimeException("계좌 정보가 존재하지 않습니다. accid = " + accid);
    }

    String encryptedAccnum = (String) result.get(0).get("accnum");
    return decrypt(encryptedAccnum); // 복호화된 계좌번호 반환
  }

}
