package mes.domain.entity;

import java.sql.Timestamp;
import java.time.LocalTime;

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
@Table(name="equip_maint")
@Setter
@Getter
@NoArgsConstructor
public class EquipmentMaint extends AbstractAuditModel{/**
	 * 
	 */
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Equipment_id\"")
	Integer Equipment_id;
	
	@Column(name = "\"MaintType\"")
	String MaintType;
	
	@Column(name = "\"DataDate\"")
	Timestamp DataDate;
	
	@Column(name = "\"MaintStartDate\"")
	Timestamp MaintStartDate;
	
	@Column(name = "\"MaintStartTime\"")
	LocalTime MaintStartTime;
	
	@Column(name = "\"MaintEndDate\"")
	Timestamp MaintEndDate;
	
	@Column(name = "\"MaintEndTime\"")
	LocalTime MaintEndTime;
	
	@Column(name = "\"Description\"")
	String Description;
	
	@Column(name = "\"ServicerName\"")
	String ServicerName;
	
	@Column(name = "\"MaintCost\"")
	Float MaintCost;
	
	@Column(name = "\"FailStartDate\"")
	Timestamp FailStartDate;
	
	@Column(name = "\"FailStartTime\"")
	LocalTime FailStartTime;
	
	@Column(name = "\"FailEndDate\"")
	Timestamp FailEndDate;
	
	@Column(name = "\"FailEndTime\"")
	LocalTime FailEndTime;
	
	@Column(name = "\"FailHr\"")
	Float FailHr;
	
	@Column(name = "\"FailDescription\"")
	String FailDescription;
}
