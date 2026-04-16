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
@Table(name="person")
@Setter
@Getter
@NoArgsConstructor
public class Person extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"ShiftCode\"")
	String shiftCode;
	
	@Column(name = "\"WorkHour\"")
	Float workHour;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"Depart_id\"")
	Integer departId;
	
	@Column(name = "\"Factory_id\"")
	Integer factoryId;
	
	@Column(name = "\"WorkCenter_id\"")
	Integer workCenterId;
	
	@Column(name = "\"Charge\"")
	String charge;
	
	@Column(name = "\"Exitdate\"")
	Timestamp exitdate;
	
	@Column(name = "\"LoginID\"")
	String loginID;
	
	@Column(name = "\"Password\"")
	String password;
	
	@Column(name = "\"PersonGroup_id\"")
	Integer personGroupId;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;

	@Column(name = "jik_id")
	String jik_id;

	@Column(name = "spjangcd")
	String spjangcd;

	@Column(name = "rtdate")
	String rtdate;

	@Column(name = "enddate")
	String enddate;

	@Column(name = "rtflag")
	String rtflag;
	
}
