package mes.domain.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.math.BigDecimal;

@Setter
@Getter
@Entity
@NoArgsConstructor
@Table(name = "tb_pb209")
public class Tb_pb209 {

    @Column(name = "spjangcd")
    String spjangcd;

    @Column(name = "id") @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    int id;

    @Column(name = "reqdate")
    String reqdate;

    @Column(name = "personid")
    Integer personid;

    @Column(name = "frdate")
    String frdate;

    @Column(name = "todate")
    String todate;

    @Column(name = "daynum")
    BigDecimal daynum;

    @Column(name = "workcd")
    String workcd;

    @Column(name = "hflag")
    String hflag;

    @Column(name = "ewolnum")
    BigDecimal ewolnum;

    @Column(name = "holinum")
    BigDecimal holinum;

    @Column(name = "monthnum")
    BigDecimal monthnum;

    @Column(name = "restnum")
    BigDecimal restnum;


}
