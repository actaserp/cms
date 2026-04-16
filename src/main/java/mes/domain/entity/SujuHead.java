package mes.domain.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.sql.Date;
import java.sql.Timestamp;

@Entity
@Table(name="suju_head")
@NoArgsConstructor
@Data
@EqualsAndHashCode( callSuper=false)
public class SujuHead extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name="\"JumunDate\"")
	Date JumunDate;

	@Column(name="\"JumunNumber\"")
	String jumunNumber;

	@Column(name="\"TotalPrice\"")
	Double TotalPrice;

	@Column(name="\"ReceivedMoney\"")
	Double ReceivedMoney;

	@Column(name="\"ReceivableMoney\"")
	Double ReceivableMoney;

	@Column(name="\"State\"")
	String State;

	@Column(name="\"ShipmentState\"")
	String ShipmentState;

	@Column(name="\"DeliveryDate\"")
	Date DeliveryDate;

	@Column(name="\"Company_id\"")
	Integer Company_id;

	String spjangcd;

	@Column(name="\"SujuType\"")
	String SujuType;

	@Column(name="\"Description\"")
	String Description;

}
