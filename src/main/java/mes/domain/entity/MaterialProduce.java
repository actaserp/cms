package mes.domain.entity;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Entity
@Table(name="mat_produce")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class MaterialProduce extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"ProcessOrder\"")
	Integer processOrder;

	@Column(name = "\"LastProcessYN\"")
	String lastProcessYN;

	@Column(name = "\"InputQty\"")
	Float inputQty;

	@Column(name = "\"TransferQty\"")
	Float transferQty;

	@Column(name = "\"LotIndex\"")
	Integer lotIndex;

	@Column(name = "\"LotNumber\"")
	String lotNumber;

	@Column(name = "\"GoodQty\"")
	Float goodQty;

	@Column(name = "\"DefectQty\"")
	Float defectQty;

	@Column(name = "\"LossQty\"")
	Float lossQty;

	@Column(name = "\"ScrapQty\"")
	Float scrapQty;

	@Column(name = "\"State\"")
	String state;

	@Column(name = "\"ProductionDate\"")
	Timestamp productionDate;

	@Column(name = "\"ShiftCode\"")
	String shiftCode;

	@Column(name = "\"StartTime\"")
	Timestamp startTime;

	@Column(name = "\"EndTime\"")
	Timestamp endTime;

	@Column(name = "\"Description\"")
	String description;

	@Column(name = "\"Actor_id\"")
	Integer actorId;

	@Column(name = "\"Equipment_id\"")
	Integer equipmentId;

	@Column(name = "\"JobResponse_id\"")
	Integer jobResponseId;

	@Column(name = "\"Material_id\"")
	Integer materialId;

	@Column(name = "\"Process_id\"")
	Integer processId;

	@Column(name = "\"StoreHouse_id\"")
	Integer storeHouseId;

	@Column(name = "\"WorkCenter_id\"")
	Integer workCenterId;

	@Column(name = "\"BomOutputAmount\"")
	Float bomOutputAmount;

	String spjangcd;
}
