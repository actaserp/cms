package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.GridColLang;

@Repository
public interface GridColLangRepository extends JpaRepository<GridColLang, Integer> {
	GridColLang getGridColLangById(Integer id);
	GridColLang findByGridColumnIdAndLangCode(Integer gridColPk, String langCode);
}
