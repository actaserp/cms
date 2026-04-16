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
@Table(name="equ_grp")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class EquipmentGroup extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"EquipmentType\"")
	String equipmentType;
	
	@Column(name = "\"Description\"")
	String description;

}
