package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.AttachFile;

@Repository 
public interface FileRepository extends JpaRepository<AttachFile, Integer> {
	
	AttachFile getFileById(Integer id);
	AttachFile getFileByTableName(Integer FileName);
	List<AttachFile> findByDataPk(int dataPk);

}
