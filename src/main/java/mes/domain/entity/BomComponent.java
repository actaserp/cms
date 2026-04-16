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
@Table(name="bom_comp")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class BomComponent extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	//@ManyToOne
	//@JoinColumn(name="\"BOM_id\"", nullable=false)	
	//Bom bom;
	
	@Column(name = "\"BOM_id\"")
	int bomId;
	
	
	//@ManyToOne
	//@JoinColumn(name="\"Material_id\"", nullable=false)	
	//Material material;
	
	@Column(name = "\"Material_id\"")
	int materialId;


	@Column(name = "\"Amount\"")
	float amount;

	@Column(name = "_creater_id")
	Integer _creater_id;

	@Column(name = "\"Description\"")
	String description;	
	
	@Column(name = "\"_order\"")
	Integer _order;

	@Column(name = "spjangcd")
	String spjangcd;
	
}
