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
@Table(name="mat_requ")
@Setter
@Getter
@NoArgsConstructor
public class MatRequ extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;

	@Column(name = "\"SourceTableName\"")
	String sourceTableName;

	@Column(name = "\"MaterialType\"")
	String materialType;

	@Column(name = "\"RequireQty1\"")
	Float requireQty1;

	@Column(name = "\"AvailableStock\"")
	Float availableStock;

	@Column(name = "\"SafetyStock\"")
	Float safetyStock;

	@Column(name = "\"ReservationStock\"")
	Float reservationStock;

	@Column(name = "\"RequireQty2\"")
	Float requireQty2;

	@Column(name = "\"RequestQty\"")
	Float requestQty;

	@Column(name = "\"RequestDate\"")
	Timestamp requestDate;

	@Column(name = "\"Material_id\"")
	Integer materialId;
}
