package mes.domain.entity;

import java.sql.Timestamp;

import javax.persistence.MappedSuperclass;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@MappedSuperclass
public abstract class AbstractAuditModel {
   public String _status;
   public Timestamp _created;
   public Timestamp _modified;

   public Integer _creater_id;
   public Integer _modifier_id;

   public void set_audit(User user) {
       Timestamp now = new Timestamp(System.currentTimeMillis());
	   
	   if(this._creater_id==null) {
		   this._creater_id = user.getId();
		   this._created = now;
	   }else {
		   this._modifier_id = user.getId();
		   this._modified = now;
	   }
   }
}
