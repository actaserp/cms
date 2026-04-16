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
@Table(name="material")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class Material extends AbstractAuditModel {
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"StoreHouseLoc\"")
	String storeHouseLoc;

	@Column(name = "\"Factory_id\"")
	Integer Factory_id;
	
	@Column(name = "\"Thickness\"")
	Float thickness;
	
	@Column(name = "\"Width\"")
	Float width;
	
	@Column(name = "\"Length\"")
	Float length;
	
	@Column(name = "\"Height\"")
	Float height;
	
	@Column(name = "\"Weight\"")
	Float weight;
	
	@Column(name = "\"Color\"")
	String color;
	
	@Column(name = "\"DefectUnitIsWeight\"")
	String defectUnitIsWeight;
	
	@Column(name = "\"Standard1\"")
	String standard1;
	
	@Column(name = "\"Standard2\"")
	String standard2;
	
	@Column(name = "\"Usage\"")
	String usage;
	
	@Column(name = "\"Class1\"")
	String class1;
	
	@Column(name = "\"Class2\"")
	String class2;
	
	@Column(name = "\"Class3\"")
	String class3;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"StandardTime\"")
	Float standardTime;
	
	@Column(name = "\"StandardTimeUnit\"")
	String standardTimeUnit;
	
	@Column(name = "\"LotSize\"")
	Float lotSize;
	
	@Column(name = "\"CustomerBarcode\"")
	String customerBarcode;
	
	@Column(name = "\"ProductionClass\"")
	String productionClass;
	
	@Column(name = "\"StockUnitQty\"")
	Float stockUnitQty;
	
	@Column(name = "\"SafetyStock\"")
	Float safetyStock;
	
	@Column(name = "\"MaxStock\"")
	Float maxStock;
	
	@Column(name = "\"ProcessSafetyStock\"")
	Float processSafetyStock;
	
	@Column(name = "\"ValidDays\"")
	Integer validDays;
	
	@Column(name = "\"PurchaseOrderStandard\"")
	String purchaseOrderStandard;
	
	@Column(name = "\"InputManCount\"")
	Integer inputManCount;
	
	@Column(name = "\"InputManHour\"")
	Float inputManHour;
	
	@Column(name = "\"PackingUnitQty\"")
	Float packingUnitQty;
	
	@Column(name = "\"PackingUnitName\"")
	String packingUnitName;
	
	@Column(name = "\"UnitPrice\"")
	Float unitPrice;
	
	@Column(name = "\"VatExemptionYN\"")
	String vatExemptionYN;
	
	@Column(name = "\"CurrentStock\"")
	Float currentStock;
	
	@Column(name = "\"CurrentStockDate\"")
	Timestamp currentStockDate;
	
	@Column(name = "\"ManagementLevel\"")
	String managementLevel;
	
	@Column(name = "\"MinOrder\"")
	Float minOrder;
	
	@Column(name = "\"MaxOrder\"")
	Float maxOrder;
	
	@Column(name = "\"LeadTime\"")
	Float leadTime;
	
	@Column(name = "\"AvailableStock\"")
	Float availableStock;
	
	@Column(name = "\"AvailableStockDate\"")
	Timestamp availableStockDate;
	
	@Column(name = "\"ExpectedOutput\"")
	Float expectedOutput;
	
	@Column(name = "\"ReservationStock\"")
	Float reservationStock;
	
	@Column(name = "\"InTestYN\"")
	String inTestYN;
	
	@Column(name = "\"OutTestYN\"")
	String outTestYN;
	
	@Column(name = "\"SalesYN\"")
	String salesYN;
	
	@Column(name = "\"CompCode\"")
	String compCode;
	
	@Column(name = "\"Routing_id\"")
	Integer routingId;
	
	@Column(name ="\"StoreHouse_id\"")
	Integer storeHouseId;
	
	@Column(name = "\"MaterialGroup_id\"")
	Integer materialGroupId;
	
	@Column(name = "\"LotUseYN\"")
	String lotUseYn;

	@Column(name = "\"Equipment_id\"")
	Integer equipment;

	@Column(name = "\"Mtyn\"")
	String mtyn;

	@Column(name = "\"Useyn\"")
	String useyn;

	@Column(name = "\"Avrqty\"")
	String avrqty;

	@Column(name = "spjangcd")
	String spjangcd;

	@Column(name = "\"Unit_id\"")
	Integer unitId;

	@Column(name = "\"WorkCenter_id\"")
	Integer workCenterId;


	//Entity 생성 후 활성화
	//MaterialGroup materialGroup;
	//Unit unit;
	//Factory factory;
	//StoreHouse storeHouse;
	//Routing routing
	//WorkCenter workCenter;
	//Equipment equipment;
}
