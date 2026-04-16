package mes.domain.entity;

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
@Table(name="process")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class Process extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	Integer id;
	
	@Column(name="\"Code\"")
	String code;
	
	@Column(name="\"Name\"")
	String name;
	
	@Column(name="\"ProcessType\"")
	String processType;
	
	@Column(name="\"Description\"")
	String description;
	
	@Column(name="\"Factory_id\"")
	Integer factory_id;

	@Column(name="spjangcd")
	String spjangcd;

}
