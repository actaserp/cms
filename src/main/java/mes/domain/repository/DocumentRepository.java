package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Document;

@Repository
public interface DocumentRepository extends JpaRepository<Document, Integer>{

	Document getDocumentById(Integer id);

}
