package mes.domain.entity;

import java.sql.Date;

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
@Table(name="haccp_diary")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class HaccpDiary extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"DataDate\"")
	Date dataDate;	
	
	@Column(name = "\"WriterName\"")
	String writerName;
	
	@Column(name = "\"Description\"")
	String description;
	

	@Column(name = "\"OverText\"")
	String overText;

	@Column(name = "\"ActionText\"")
	String actionText;

	@Column(name = "\"ActionUserName\"")
	String actionUserName;

    @Column(name = "\"ConfirmUserName\"")
	String confirmUserName;

	@Column(name = "\"HaccpProcess_id\"")
	int haccpProcess_id;


}
