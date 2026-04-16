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
@Table(name="mat_lot_cons")
@Setter
@Getter
@NoArgsConstructor
public class MatLotCons extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"OutputDateTime\"")
	Timestamp outputDateTime;

	@Column(name = "\"OutputQty\"")
	Float outputQty;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
	
	@Column(name = "\"MaterialLot_id\"")
	Integer materialLotId;
	
	@Column(name = "\"PrevStock\"")
	Float prevStock;
	
	@Column(name = "\"CurrentStock\"")
	Float currentStock;

	String spjangcd;
}
