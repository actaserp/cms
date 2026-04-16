package mes.domain.entity;

import java.sql.Timestamp;

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
@Table(name="mat_consu")
@Setter
@Getter
@NoArgsConstructor
public class MaterialConsume extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"ProcessOrder\"")
	Integer processOrder;
	
	@Column(name = "\"LotIndex\"")
	Integer lotIndex;

	@Column(name = "\"BomQty\"")
	Float bomQty;
	
	@Column(name = "\"ConsumedQty\"")
	Float consumedQty;
	
	@Column(name = "\"ScrapQty\"")
	Float scrapQty;
	
	@Column(name = "\"AddQty\"")
	Float addQty;
	
	@Column(name = "\"State\"")
	String State;

	@Column(name = "\"StartTime\"")
	Timestamp startTime;
	
	@Column(name = "\"EndTime\"")
	Timestamp endTime;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"JobResponse_id\"")
	Integer jobResponseId;
	
	@Column(name = "\"Material_id\"")
	Integer materialId;

	@Column(name = "\"StoreHouse_id\"")
	Integer storeHouseId;
	
	@Column(name = "\"LotNumber\"")
	String lotNumber;

	String spjangcd;

}
