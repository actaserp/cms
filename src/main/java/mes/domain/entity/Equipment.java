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
@Table(name="equ")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class Equipment extends AbstractAuditModel{
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"Maker\"")
	String maker;
	
	@Column(name = "\"Model\"")
	String model;
	
	@Column(name = "\"Standard\"")
	String standard;
	
	@Column(name = "\"ManageNumber\"")
	String manageNumber;
	
	@Column(name = "\"SerialNumber\"")
	String serialNumber;
	
	@Column(name = "\"ProductionYear\"")
	Integer productionYear;
	
	@Column(name = "\"AssetYN\"")
	String assetYN;
	
	@Column(name = "\"DurableYears\"")
	Integer durableYears;
	
	@Column(name = "\"PowerWatt\"")
	Integer powerWatt;
	
	@Column(name = "\"Manager\"")
	String manager;
	
	@Column(name = "\"SupplierName\"")
	String supplierName;
	
	@Column(name = "\"PurchaseDate\"")
	Timestamp purchaseDate;
	
	@Column(name = "\"PurchaseCost\"")
	Float purchaseCost;
	
	@Column(name = "\"ServiceCharger\"")
	String serviceCharger;
	
	@Column(name = "\"InstallDate\"")
	Timestamp installDate;
	
	@Column(name = "\"DisposalDate\"")
	Timestamp disposalDate;
	
	@Column(name = "\"DisposalReason\"")
	String disposalReason;
	
	@Column(name = "\"OperationRateYN\"")
	String operationRateYN;
	
	@Column(name = "\"Status\"")
	String status;
	
	@Column(name = "\"EquipmentGroup_id\"")
	Integer equipmentGroup_id;
	
	@Column(name = "\"WorkCenter_id\"")
	Integer workCenter_id;

	@Column(name = "\"Voltage\"")
	String voltage;

	@Column(name = "\"Inputdate\"")
	String inputDate;

	@Column(name = "\"DepartName\"")
	String departName;

	@Column(name = "\"Usage\"")
	String usage;

	@Column(name = "\"ASTelNumber\"")
	String astelNumber;

	@Column(name = "\"AttentionRemark\"")
	String attentionRemark;

	@Column(name = "\"OvenCount\"")
	Integer ovenCount;

	@Column(name = "\"OvenTemperCount\"")
	Integer ovenTemperCount;

	@Column(name = "\"OvenProductTemperStandard\"")
	Double ovenProductTemperStandard;

	@Column(name = "\"OvenHeatingMnStandard\"")
	Double ovenHeatingMnStandard;

	@Column(name = "\"CcpTestCycle\"")
	Double ccpTestCycle;

	@Column(name = "\"MetalDetectMaster_id\"")
	Integer metalDetectMaster_id;

	@Column(name = "\"ForeignDetectMaster_id\"")
	Integer foreignDetectMaster_id;

	@Column(name = "\"Depart_id\"")
	Integer depart_id;
}
