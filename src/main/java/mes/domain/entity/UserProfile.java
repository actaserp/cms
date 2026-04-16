package mes.domain.entity;

import java.io.Serializable;
import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonBackReference;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Entity
@Table(name="user_profile")
@EqualsAndHashCode(callSuper=false)
public class UserProfile extends AbstractAuditModel implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"CompCode\"")
	String compCode;
	
	@Column(name = "\"lang_code\"")
	String LangCode;
	
	@Column(name = "\"Company_id\"")
	Integer CompanyId;
	
	@Column(name = "\"Depart_id\"")
	Integer DepartId;
	
	@Column(name = "\"Factory_id\"")
	Integer FactoryId;
	
	@Column(name = "password_changed")
	Timestamp PasswordChanged;
	
	@Column(name = "\"token\"")
	String Token;
	
	@JsonBackReference
	@Id
	@OneToOne
	@JoinColumn(name = "\"User_id\"")
	private User user;
	
	
	@ManyToOne
	@JoinColumn(name="\"UserGroup_id\"")
	UserGroup userGroup;
}
