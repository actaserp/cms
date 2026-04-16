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
@Table(name="defect_type")
@NoArgsConstructor
@Data
@EqualsAndHashCode( callSuper=false)
public class DefectType extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name="\"Code\"")
	String code;
	
	@Column(name="\"Name\"")
	String name;
	
	@Column(name="\"Coverage\"")
	String coverage="all";
	
	@Column(name = "\"Description\"")
	String description;
	
}
