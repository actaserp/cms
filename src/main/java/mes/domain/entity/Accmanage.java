package mes.domain.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(name="tb_accmanage")
@NoArgsConstructor
@Data
@EqualsAndHashCode( callSuper=false)
public class Accmanage {

    @EmbeddedId
    private AccmanageId id; // 복합키

    @Column(name = "itemnm")
    String itemnm; //관리항목명

    @Column(name = "essyn")
    String essyn; //필수여부

    @Column(name = "useyn")
    String useyn; //사용여부


}
