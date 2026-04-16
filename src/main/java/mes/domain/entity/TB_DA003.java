package mes.domain.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(name="TB_DA003")
@NoArgsConstructor
@Data
public class TB_DA003 {
  @EmbeddedId
  private TB_DA003Id id;

  @Column(name = "projnm")
  private String projnm;  //프로젝트 명칭

  @Column(name = "balcltcd")
  private Integer balcltcd; //발주퍼 코드

  @Column(name = "balcltnm")
  private String balcltnm;

  @Column(name = "actnm")
  private String actnm;

  @Column(name = "stdate")
  private String stdate;

  @Column(name = "eddate")
  private String eddate;

  @Column(name = "contdate")
  private String contdate;

  @Column(name = "endflag")
  private String endflag;

  @Column(name = "remark")
  private String remark;

  @Column(name = "prodivicd")
  private Integer prodivicd;

  @Column(name = "prodivinmn")
  private String prodivinmn;

  @Column(name = "indate")
  private String indate;

  @Column(name = "inuserid")
  private String inuserid;
}
