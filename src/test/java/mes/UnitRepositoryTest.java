package mes;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import mes.domain.entity.Unit;
import mes.domain.repository.UnitRepository;

@SpringBootTest
public class UnitRepositoryTest {

	@Autowired	
	UnitRepository unitRepository;
	
	
	@Test	
	public void getUnitByIdTest()
	{
		Optional<Unit> optUnit = unitRepository.findById(1);
		
		if(optUnit.isPresent()) {
			Unit u = optUnit.get();
			System.out.println(u);
			
		    List<Unit> unitList = this.unitRepository.findByName("BOX");
		    if(unitList.size()>0) {
		    	System.out.println(unitList);
		    }
		}
		
		Unit uu = this.unitRepository.getUnitById(1);
		System.out.println(uu);
		
	}
	
}
