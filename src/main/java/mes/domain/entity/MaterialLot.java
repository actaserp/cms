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
@Table(name="mat_lot")
@Setter
@Getter
@NoArgsConstructor
public class MaterialLot extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"LotNumber\"")
	String lotNumber;
	
	@Column(name = "\"InputDateTime\"")
	Timestamp inputDateTime;
	
	@Column(name = "\"InputQty\"")
	Float inputQty;
	
	@Column(name = "\"OutQtySum\"")
	Float outQtySum;
	
	@Column(name = "\"CurrentStock\"")
	Float currentStock;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
	
	@Column(name = "\"Material_id\"")
	Integer materialId;
	
	@Column(name = "\"EffectiveDate\"")
	Timestamp effectiveDate;
	
	@Column(name = "\"StoreHouse_id\"")
	Integer storeHouseId;

	String spjangcd;
}
