package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Rela3Data;

@Repository
public interface Rela3DataRepository extends JpaRepository<Rela3Data, Integer>{

	List<Rela3Data> findByRelationNameAndDataPk1AndTableName1AndTableName2AndTableName3(String string, Integer id,
			String string2, String string3, String string4);

}
