package mes.domain.entity.mobile;

import lombok.*;

import javax.persistence.*;
import java.math.BigDecimal;

@Setter
@Getter
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "tb_pb204")
public class TB_PB204 {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private Integer id;

  @Column(name = "spjangcd", length = 2, nullable = false)
  private String spjangcd;

  @Column(name = "reqdate", length = 8)
  private String reqdate;

  @Column(name = "personid")
  private Integer personid;

  @Column(name = "frdate", length = 8)
  private String frdate;

  @Column(name = "todate", length = 8)
  private String todate;

  @Column(name = "sttime", length = 5)
  private String sttime;

  @Column(name = "edtime", length = 5)
  private String edtime;

  @Column(name = "daynum", precision = 5, scale = 2)
  private BigDecimal daynum;

  @Column(name = "workcd", length = 2)
  private String workcd;

  @Column(name = "remark", length = 50)
  private String remark;

  @Column(name = "appdate", length = 8)
  private String appdate;

  @Column(name = "appnum", length = 20)
  private String appnum;

  @Column(name = "appgubun", length = 3)
  private String appgubun;

  @Column(name = "fixflag", length = 1)
  private String fixflag;

  @Column(name = "appperid")
  private Integer appperid;

  @Column(name = "appuserid", length = 50)
  private String appuserid;

  @Column(name = "yearflag", length = 1)
  private String yearflag;
}
