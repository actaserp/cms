package mes.domain.entity.approval;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.time.LocalDateTime;


@Entity
@Table(name = "TB_AA009")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TB_AA009 {

  @EmbeddedId
  private TB_AA009_PK id;

  @Column(name = "tiosec")
  private String tiosec;

  @Column(name = "cashyn")
  private String cashyn;

  @Column(name = "busipur")
  private String busipur;

  @Column(name = "mssec")
  private String mssec;

  @Column(name = "spoccu")
  private String spoccu;

  @Column(name = "remark")
  private String remark;

  @Column(name = "taxdate")
  private String taxdate;

  @Column(name = "taxnum")
  private String taxnum;

  @Column(name = "regdate")
  private String regdate;

  @Column(name = "subject")
  private String subject;

  @Column(name = "bsdate")
  private String bsdate;

  @Column(name = "bseccd")
  private String bseccd;

  @Column(name = "busicd")
  private String busicd;

  @Column(name = "setnum")
  private String setnum;

  @Column(name = "spjangnm")
  private String spjangnm;

  @Column(name = "busicd_cnt")
  private String busicdCnt;

  @Column(name = "cdflag")
  private String cdflag;

  @Column(name = "orgspdate")
  private String orgspdate;

  @Column(name = "orgspnum")
  private String orgspnum;

  @Column(name = "copydate")
  private String copydate;

  @Column(name = "appdate")
  private String appdate;

  @Column(name = "appperid")
  private String appperid;

  @Column(name = "appgubun")
  private String appgubun;

  @Column(name = "appnum")
  private String appnum;

  @Column(name = "fixflag")
  private String fixflag;

  @Column(name = "inputsabun")
  private String inputsabun;

  @Column(name = "inputdate")
  private LocalDateTime inputdate;

  @Column(name = "inputid")
  private String inputid;
}
