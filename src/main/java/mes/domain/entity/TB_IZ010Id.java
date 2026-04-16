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
public class TB_IZ010Id implements Serializable {

  private String spjangcd; // 사업장
  private String cardnum; // 신용카드번호

}
