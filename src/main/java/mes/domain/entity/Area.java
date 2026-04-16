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
@Table(name="area")
@NoArgsConstructor
@Data
@EqualsAndHashCode( callSuper=false)
public class Area extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name="\"Factory_id\"")
	Integer factory_id;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name="\"Parent_id\"")
	Integer parent_id;
	
	@Column(name = "\"Description\"")
	String description;

	@Column(name = "spjangcd")
	String spjangcd;
	
}