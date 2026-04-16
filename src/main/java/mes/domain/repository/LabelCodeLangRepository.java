package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.LabelCodeLang;

@Repository
public interface LabelCodeLangRepository extends JpaRepository<LabelCodeLang, Integer>{

	LabelCodeLang getLabelCodeLangById(Integer id);

	List<LabelCodeLang> findByLangCodeAndLabelCodeId(String langCode, int id);

}
