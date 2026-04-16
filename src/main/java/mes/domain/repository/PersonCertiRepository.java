package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.PersonCerti;


@Repository
public interface PersonCertiRepository extends JpaRepository<PersonCerti, Integer>{

	List<PersonCerti> findByPersonNameAndCertificateCode(String personName, String string);


}
