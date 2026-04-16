package mes.domain.entity.approval;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Embeddable
@Getter
@Setter
@EqualsAndHashCode
public class TB_PB204_PK implements Serializable {

  @Column(name = "custcd")
  private String custcd;

  @Column(name = "spjangcd")
  private String spjangcd;

  @Column(name = "vayear")
  private String vayear;

  @Column(name = "vanum")
  private String vanum;

  @Column(name = "perid")
  private String perid;
}

