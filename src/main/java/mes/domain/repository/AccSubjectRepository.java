package mes.domain.repository;

import mes.domain.entity.Accsubject;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface AccSubjectRepository extends JpaRepository<Accsubject, String> {


}
