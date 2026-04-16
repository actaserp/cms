package mes.domain.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name="mat_comp_uprice")
@Setter
@Getter
@NoArgsConstructor
public class MatCompUprice extends  AbstractAuditModel{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;


    @Column(name = "\"UnitPrice\"", nullable = false)
    private Double unitPrice;

    @Column(name = "\"FormerUnitPrice\"")
    private Double formerUnitPrice;

    @Column(name = "\"ApplyStartDate\"", nullable = false)
    private Timestamp applyStartDate;

    @Column(name = "\"ApplyEndDate\"", nullable = false)
    private Timestamp applyEndDate;

    @Column(name = "\"ChangeDate\"")
    private Timestamp changeDate;

    @Column(name = "\"ChangerName\"", length = 30)
    private String changerName;

    @Column(name = "\"Company_id\"", nullable = false)
    private Integer companyId;

    @Column(name = "\"Material_id\"", nullable = false)
    private Integer materialId;

    @Column(name = "\"Type\"", length = 2)
    private String type;
}
