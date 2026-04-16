package mes.domain.entity;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
//import javax.persistence.JoinColumn;
//import javax.persistence.ManyToOne;
import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Entity
@Table(name="bom")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class Bom extends AbstractAuditModel{	
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name = "\"Name\"")
	String name;	

	//@ManyToOne
	//@JoinColumn(name="\"Material_id\"", nullable=false)	
	//Material material;
	
	@Column(name = "\"Material_id\"")
	int materialId;

	@Column(name = "\"OutputAmount\"")
	Float OutputAmount;
	
	@Column(name = "\"Version\"")
	String version="1.0";	
	
	@Column(name = "\"BOMType\"")
	String bomType="manufacturing";	
	
	@Column(name = "\"StartDate\"")
	Timestamp startDate;
	
	@Column(name = "\"EndDate\"")
	Timestamp endDate;

	@Column(name="spjangcd")
	String spjangcd;


}
