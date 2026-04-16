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
@Table(name="doc_result")
@Setter
@Getter
@NoArgsConstructor
public class Document extends AbstractAuditModel {/**
	 * 
	 */
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"DocumentForm_id\"")
	Integer DocumentForm_id;
	
	@Column(name = "\"DocumentName\"")
	String DocumentName;
	
	@Column(name = "\"Content\"")
	String Content;
	
	@Column(name = "\"DocumentDate\"")
	Timestamp DocumentDate;
	
}
