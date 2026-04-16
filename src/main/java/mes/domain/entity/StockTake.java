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

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Entity
@Table(name="stock_take")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class StockTake extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"TakeDate\"")
	LocalDate takeDate;
	
	@Column(name = "\"TakeTime\"")
	LocalTime takeTime;
	
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
	
	@Column(name = "\"Material_id\"")
	Integer materialId;
	
	@Column(name = "\"StoreHouse_id\"")
	Integer storeHouseId;

	String spjangcd;
}
