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
@Table(name="cust_complain")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class CustComplain extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name="\"Type\"")
	String type;
	
	@Column(name="\"Title\"")
	String title;
	
	@Column(name="\"Qty\"")
	Integer qty;
	
	@Column(name="\"Content\"")
	String content;
	
	@Column(name="\"ReceiveDate\"")
	Date receiveDate;
	
	@Column(name="\"Material_id\"")
	Integer materialId;
	
	@Column(name="\"CheckName\"")
	String checkName;
	
	@Column(name="\"CheckDate\"")
	Date checkDate;
	
	@Column(name="\"FinishDate\"")
	Date finishDate;
	
	@Column(name="\"CheckState\"")
	String checkState;
}
