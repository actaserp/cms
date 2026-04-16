package mes.domain.entity;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name="mat_inout")
@Setter
@Getter
@NoArgsConstructor
public class MaterialInout extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"InoutDate\"")
	LocalDate inoutDate;

	@Column(name = "\"InoutTime\"")
	LocalTime inoutTime;

	@Column(name = "\"InOut\"")
	String inOut;

	@Column(name = "\"InputType\"")
	String inputType;

	@Column(name = "\"PotentialInputQty\"")
	Float potentialInputQty;

	@Column(name = "\"DefectQty\"")
	Float defectQty;

	@Column(name = "\"InputQty\"")
	Float inputQty;

	@Column(name = "\"UnitPrice\"")
	Float unitPrice;

	@Column(name = "\"TotalPrice\"")
	Float totalPrice;

	@Column(name = "\"LotNumber\"")
	String lotNumber;

	@Column(name = "\"OutputType\"")
	String outputType;

	@Column(name = "\"OutputQty\"")
	Float outputQty;

	@Column(name = "\"ReservedOutQty\"")
	Float reservedOutQty;

	@Column(name = "\"State\"")
	String state;

	@Column(name = "\"Actor_id\"")
	Integer actorId;

	@Column(name = "\"Description\"")
	String description;

	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;

	@Column(name = "\"SourceTableName\"")
	String sourceTableName;

	@Column(name = "\"LastStock\"")
	Float lastStock;

	@Column(name = "\"LastAvailableStock\"")
	Float lastAvailableStock;

	@Column(name = "\"LastReservationStock\"")
	Float lastReservationStock;

	@Column(name = "\"LastHouseStock\"")
	Float lastHouseStock;

	@Column(name = "\"Company_id\"")
	Integer companyId;

	@Column(name = "\"Material_id\"")
	Integer materialId;

	@Column(name = "\"MaterialInoutHead_id\"")
	Integer materialInoutHeadId;

	@Column(name = "\"MaterialOrder_id\"")
	Integer materialOrderId;

	@Column(name = "\"StoreHouse_id\"")
	Integer storeHouseId;
	
	@Column(name = "\"EffectiveDate\"")
	Timestamp effectiveDate;

	String spjangcd;
	
}
