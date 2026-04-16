package mes.domain.entity;

import java.time.LocalTime;

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
@Table(name="shift")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class Shift extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"StartTime\"")
	LocalTime startTime;
	
	@Column(name = "\"EndTime\"")
	LocalTime endTime;
	
	@Column(name = "\"Description\"")
	String description;

	@Column(name = "spjangcd")
	String spjangcd;


}
