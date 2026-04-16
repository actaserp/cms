package mes.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Embeddable;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
public class TB_DA003Id implements Serializable {

  private String spjangcd;  //사업장
  private String projno;  //프로젝트 번호
}


