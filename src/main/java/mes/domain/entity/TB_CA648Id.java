package mes.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
public class TB_CA648Id implements Serializable {

  @Column(name = "spjangcd") // 사업장
  private String spjangcd;

  @Column(name = "gartcd") // 비용그룹코드
  private String gartcd;

  @Column(name = "artcd") // 비용세코드
  private String artcd;
}
