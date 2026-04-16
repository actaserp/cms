package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.AttachFile;

@Repository
public interface AttachFileRepository extends JpaRepository<AttachFile, Integer> {
	
	AttachFile getAttachFileByDataPk(Integer DataPk);
	AttachFile getAttachFileByTableName(String tableName);
	AttachFile getAttachFileByIdAndDataPk(Integer id,Integer DataPk);
	
	List<AttachFile> getAttachFileByTableNameAndDataPkAndAttachName(String tableName, Integer dataPk, String attachName);
	List<AttachFile> findByIdAndAttachName(String fileId1, String string);
	
	List<AttachFile> findByTableNameAndDataPkAndAttachNameAndFileIndex(String tableName, Integer dataPk, String attachName,
			int i);

}
