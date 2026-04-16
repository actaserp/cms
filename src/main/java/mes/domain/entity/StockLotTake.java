package mes.domain.entity;

import java.sql.Date;
import java.sql.Time;
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
@Table(name="stock_lot_take")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class StockLotTake extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"MaterialLot_id\"")
	Integer materialLotId;

	@Column(name = "\"StoreHouse_id\"")
	Integer storeHouseId;	
	
	@Column(name = "\"TakeDate\"")
	Date takeDate;
	
	@Column(name = "\"TakeTime\"")
	Time takeTime;
	
	@Column(name = "\"AccountStock\"")
	Float accountStock;
	
	@Column(name = "\"RealStock\"")
	Float realStock;
	
	@Column(name = "\"Gap\"")
	Float gap;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"State\"")
	String state;
	
	@Column(name = "\"Taker_id\"")
	Integer taker_id;
	
	@Column(name = "\"Confirmer_id\"")
	Integer confirmerId;
	
	@Column(name = "\"ConfirmDateTime\"")
	Timestamp confirmDateTime;

	String spjangcd;
	
}
