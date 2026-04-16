package mes.domain.entity;

import java.util.Date;

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
@Table(name="doc_form")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class DocForm extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"FormType\"")
	String formType;
	
	@Column(name = "\"FormGroup\"")
	String formGroup;
	
	@Column(name = "\"FormName\"")
	String formName;
	
	@Column(name = "\"Content\"")
	String content;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"State\"")
	String state;
	
	@Column(name = "\"StartDate\"")
	Date startDate;
	
	@Column(name = "\"EndDate\"")
	Date endDate;
}
