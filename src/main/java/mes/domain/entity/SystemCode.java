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
@Table(name="sys_code")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class SystemCode extends AbstractAuditModel {
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;

	@Column(name="\"CodeType\"")
	String codeType;

	@Column(name="\"Code\"")
	String code;

	@Column(name="\"Value\"")
	String value;

	@Column(name="\"Description\"")
	String description;

	@Column(name="\"_ordering\"")
	Integer _ordering;

	@Column(name = "spjangcd")
	String spjangcd;
}
