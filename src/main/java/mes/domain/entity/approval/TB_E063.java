package mes.domain.entity.approval;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "tb_e063") // DB 테이블명
@Setter
@Getter
@NoArgsConstructor
public class TB_E063 {

    @EmbeddedId
    private TB_E063_PK id;

    @Column(length = 50) // DB에 정의된 remark의 길이는 50
    private String remark;

}
