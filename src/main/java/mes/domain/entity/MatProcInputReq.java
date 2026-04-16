package mes.domain.entity;


import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name="mat_proc_input_req")
@Setter
@Getter
@NoArgsConstructor
public class MatProcInputReq extends AbstractAuditModel{
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"RequestDate\"")
	Timestamp requestDate;
	
	@Column(name = "\"Requester_id\"")
	Integer requesterId;
}
