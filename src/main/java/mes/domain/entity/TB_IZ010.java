package mes.domain.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(name="tb_iz010")
@NoArgsConstructor
@Data
public class TB_IZ010 {

  @EmbeddedId
  private TB_IZ010Id id;

  @Column(name = "cardco")
  private String cardco; // 카드사코드

  @Column(name = "cardnm")
  private String cardnm; // 카드사명칭

  @Column(name = "cdperid")
  private Integer cdperid; // 카드사용직원

  @Column(name = "cdpernm")
  private String cdpernm; // 카드사용직원명

  @Column(name = "issudate")
  private String issudate; // 발급일자

  @Column(name = "expdate")
  private String expdate; // 만기일자

  @Column(name = "stldate")
  private String stldate; // 결재일자

  @Column(name = "bankid")
  private Integer bankid; // 결제은행코드

  @Column(name = "banknm")
  private String banknm; // 결제은행명

  @Column(name = "accid")
  private Integer accid; // 결제계좌코드

  @Column(name = "accnum")
  private String accnum; // 결제계좌번호

  @Column(name = "baroflag")
  private String baroflag; // 카드연동

  @Column(name = "cardwebid")
  private String cardwebid; // 각 카드사 홈페이지아이디

  @Column(name = "cardwebpw")
  private String cardwebpw; // 각 카드사 홈페이지비밀번호

  @Column(name = "barocd")
  private String barocd; // 바로빌카드코드

  @Column(name = "baroid")
  private String baroid; // 바로빌아이디

  @Column(name = "useyn")
  private String useyn; // 사용여부

  @Column(name = "indate")
  private String indate; // 입력일자

  @Column(name = "inuserid")
  private String inuserid; // 입력아이디

  @Column(name = "cardclafi")
  private String cardclafi; //결재구분  법인 :1, 개인: 2
}
