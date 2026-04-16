package mes.domain.entity;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_salesdetail")
public class TB_SalesDetail {

    @EmbeddedId
    private TB_SalesDetailId id;

    private String misdate;
    private String itemnm;
    private String spec;

    private Integer qty;
    private Integer unitcost;
    private Integer supplycost;
    private Integer taxtotal;
    private Integer totalamt;

    private String remark;

    private String purchasedt;

    @Column(name = "\"Material_id\"")
    private Integer materialId;

    private String spjangcd;

    @Version
    private Integer vercode;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumns({
            @JoinColumn(name = "misnum", referencedColumnName = "misnum", insertable = false, updatable = false)
    })
    private TB_Salesment salesment;
}
