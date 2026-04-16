package mes.domain.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name="inspection_reports")
@NoArgsConstructor
@Data
public class inspection_reports extends AbstractAuditModel{

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  int id;

  @Column(name = "\"ProcessOrder\"")
  Integer processOrder;

  @Column(name = "\"LotIndex\"")
  Integer lotIndex;

  @Column(name = "\"DefectQty\"")
  Double defectQty;

  @Column(name = "\"Description\"")
  String description;

  @Column(name = "\"DefectType_id\"")
  Integer defectTypeId;

  @Column(name = "\"DetailDataPk\"")
  Integer detailDataPk;

  @Column(name = "\"DetailTableName\"")
  String detailTableName;

  @Column(name="\"InspectionDate\"")
  Date inspectiondate;

  @Column(name="\"WorkCenter_id\"")
  Integer workCenter_id;

  @Column(name="\"InspectionQty\"")
  Double inspectionQty;

  String spjangcd;

}
