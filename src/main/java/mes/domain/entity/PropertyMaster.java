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
@Table(name="prop_master")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class PropertyMaster extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"TableName\"")
	String tableName;
	

	@Column(name = "\"Code\"")
	String code;

	@Column(name = "\"Type\"")
	String type;

	@Column(name = "\"Description\"")
	String description;
}
