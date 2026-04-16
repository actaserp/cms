package mes.domain.entity;

import java.io.Serializable;
import java.sql.Timestamp;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name="auth_user")
@Setter
@Getter
@NoArgsConstructor
public class User implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	String password;
	
	@JsonIgnore
	String username;
	
	String email;

	@Column(name = "is_superuser")
	Boolean superUser;
	
	@Column(name = "is_active")
	Boolean active;
	
	@Column(name = "first_name")
	String first_name;
	
	@Column(name = "last_name")
	String last_name;
	
	@Column(name = "is_staff")
	Boolean is_staff;
	
	@Column(name = "last_login")
	Timestamp lastLogin;
	
	@Column(name = "date_joined")
	Timestamp date_joined;

	@Column(name = "spjangcd")
	String spjangcd;

	@Column(name = "personid")
	Integer personid;

	@Column(name = "tel")
	String tel;

	
	@JsonManagedReference
	@OneToOne(mappedBy = "user", cascade = CascadeType.ALL)
	private UserProfile userProfile;
	
}
