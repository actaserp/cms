package mes.domain.entity;

import java.sql.Timestamp;

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
@Table(name="mat_proc_input_req")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class MaterialProcessInputRequest extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name = "\"RequestDate\"")
	Timestamp requestDate;
	
	@Column(name = "\"Requester_id\"")
	Integer requesterId;
}
