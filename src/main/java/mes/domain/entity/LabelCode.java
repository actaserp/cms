package mes.domain.entity;


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
@Table(name="label_code")
@Setter
@Getter
@NoArgsConstructor
public class LabelCode extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"ModuleName\"")
	String moduleName;
	
	@Column(name = "\"TemplateKey\"")
	String templateKey;
	
	@Column(name = "\"LabelCode\"")
	String labelCode;
	
	@Column(name = "\"Description\"")
	String description;
	
}
