package mes.domain.entity.approval;

import groovy.transform.EqualsAndHashCode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Embeddable
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class TB_AA009_PK  implements Serializable {

  @Column(name = "custcd")
  private String custcd;

  @Column(name = "spjangcd")
  private String spjangcd;

  @Column(name = "spdate")
  private String spdate;

  @Column(name = "spnum")
  private String spnum;
}