package mes.domain.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name="tb_ca648")
@NoArgsConstructor
@Data
public class TB_CA648 {

  @EmbeddedId
  private TB_CA648Id id;

  @Column(name = "artnm") // 비용상세명칭
  private String artnm;

  @Column(name = "jiflag") // 고정비
  private String jiflag;

  @Column(name = "gflag") // 비용분류
  private String gflag;

  @Column(name = "acccd") // 비용계정코드
  private String acccd;

  @Column(name = "accnm") // 비용계정명
  private String accnm;

  @Column(name = "wacccd") // 원가계정코드
  private String wacccd;

  @Column(name = "waccnm") // 원가계정명
  private String waccnm;

  @Column(name = "sacccd") // 상대계정코드
  private String sacccd;

  @Column(name = "saccnm") // 상대계정명
  private String saccnm;

  @Column(name = "useyn") // 사용여부
  private String useyn;

  @Column(name = "indate") // 입력일자
  private String indate;

  @Column(name = "inuserid") // 입력아이디
  private String inuserid;

}
