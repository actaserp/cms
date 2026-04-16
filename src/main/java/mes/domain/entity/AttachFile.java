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
@Table(name="attach_file")
@NoArgsConstructor
@Data
@EqualsAndHashCode( callSuper=false)
public class AttachFile extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name="\"TableName\"")
	String tableName;
	
	@Column(name="\"DataPk\"")
	Integer dataPk;
	
	@Column(name="\"AttachName\"")
	String attachName;
	
	@Column(name="\"FileIndex\"")
	Integer fileIndex;
	
	@Column(name="\"FileName\"")
	String fileName;
	
	@Column(name="\"PhysicFileName\"")
	String physicFileName;
	
	@Column(name="\"ExtName\"")
	String extName;
	
	@Column(name="\"FilePath\"")
	String filePath;
	
	@Column(name="\"FileSize\"")
	Integer fileSize;

}
