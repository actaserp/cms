package mes.domain.entity;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_invoicement")
public class TB_Invoicement {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer misnum;

    private String misdate;
    private String misgubun;
    private Integer cltcd;
    private Integer paycltcd;
    private Integer supplycost;
    private Integer taxtotal;
    private Integer totalamt;
    private String remark1;
    private String remark2;
    private String remark3;
    private String deductioncd;
    private Integer depart_id;
    private Integer card_id;
    private String title;
    private String spjangcd;
    private String cltflag;
    private String paycltflag;

    @Version
    private Integer vercode;

    @OneToMany(mappedBy = "invoicement", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<TB_InvoiceDetail> details = new ArrayList<>();
}
