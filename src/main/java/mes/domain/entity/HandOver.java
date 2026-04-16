package mes.domain.entity;

import java.util.Date;

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
@Table(name="hand_over")
@Setter
@Getter
@NoArgsConstructor
public class HandOver extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"StartDate\"")
	Date startDate;
	
	@Column(name = "\"EndDate\"")
	Date endDate;
	
	@Column(name = "\"FromName\"")
	String FromName;
	
	@Column(name = "\"FromDataPk\"")
	Integer fromDataPk;
	
	@Column(name = "\"FromTableName\"")
	String fromTableName;
	
	@Column(name = "\"FromTel\"")
	String fromTel;
	
	@Column(name = "\"ToName\"")
	String toName;
	
	@Column(name = "\"ToDataPk\"")
	Integer toDataPk;
	
	@Column(name = "\"ToTableName\"")
	String toTableName;
	
	@Column(name = "\"ToTel\"")
	String toTel;
	
	@Column(name = "\"Reason\"")
	String reason;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"_status\"")
	String _status;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
}
