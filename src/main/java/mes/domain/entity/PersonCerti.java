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
@Table(name="person_certi")
@Setter
@Getter
@NoArgsConstructor
public class PersonCerti extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"PersonName\"")
	String personName;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
	
	@Column(name = "\"CertificateCode\"")
	String certificateCode;
	
	@Column(name = "\"TestDate\"")
	Timestamp testDate;
	
	@Column(name = "\"IssueDate\"")
	Timestamp issueDate;
	
	@Column(name = "\"ExpireDate\"")
	Timestamp expireDate;
	
	@Column(name = "\"NextTestDate\"")
	Timestamp nextTestDate;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"_status\"")
	String _status;
}
