package mes.domain.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import mes.domain.entity.commute.TB_PB201_PK;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.math.BigDecimal;

@Setter
@Getter
@Entity
@NoArgsConstructor
@Table(name = "tb_pb210")
public class Tb_pb210 {

    @EmbeddedId
    private Tb_pb210Id id;

    @Column(name = "worknm")
    String worknm;

    @Column(name = "remark")
    String remark;

    @Column(name = "yearflag")
    String yearflag;

    @Column(name = "usenum")
    BigDecimal usenum;


}
