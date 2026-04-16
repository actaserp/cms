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
@Table(name="mat_proc_input")
@Setter
@Getter
@NoArgsConstructor
public class MatProcInput extends AbstractAuditModel{
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"RequestQty\"")
	Float requestQty;
	
	@Column(name = "\"InputQty\"")
	Float inputQty;
	
	@Column(name = "\"State\"")
	String state;
	
	@Column(name = "\"InputDateTime\"")
	Timestamp inputDateTime;
	
	@Column(name = "\"Actor_id\"")
	Integer actorId;
	
	@Column(name = "\"Material_id\"")
	Integer materialId;
	
	@Column(name = "\"MaterialProcessInputRequest_id\"")
	Integer materialProcessInputRequestId;
	
	@Column(name = "\"MaterialStoreHouse_id\"")
	Integer materialStoreHouseId;
	
	@Column(name = "\"ProcessStoreHouse_id\"")
	Integer processStoreHouseId;
	
	@Column(name = "\"MaterialLot_id\"")
	Integer materialLotId;
}
