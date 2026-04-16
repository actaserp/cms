package mes.app.definition;

import mes.app.definition.service.FactoryService;
import mes.domain.entity.Factory;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.FactoryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/definition/factory")
public class factoryController {

    @Autowired
    FactoryService factoryService;

    @Autowired
    FactoryRepository factoryRepository;

    @GetMapping("/read")
    public AjaxResult getDepart(
            @RequestParam("keyword") String keyword,
            @RequestParam(value ="spjangcd") String spjangcd) {

        List<Map<String, Object>> items = this.factoryService.getFactory(keyword,spjangcd);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    @GetMapping("/detail")
    public AjaxResult getDepartDetail(
            @RequestParam("id") int id,
            HttpServletRequest request) {

        Map<String, Object> item = this.factoryService.getFactoryDetail(id);

        AjaxResult result = new AjaxResult();
        result.data = item;

        return result;
    }

    @PostMapping("/save")
    public AjaxResult saveDepart(
            @RequestParam(value="id", required=false) Integer id,
            @RequestParam(value="Code", required=false) String code,
            @RequestParam(value="Description", required=false) String description,
            @RequestParam(value="Name", required=false) String name,
            @RequestParam(value="Type", required=false) String type,
            @RequestParam(value ="spjangcd") String spjangcd,
            HttpServletRequest request,
            Authentication auth
    ) {

        User user = (User)auth.getPrincipal();

        Factory f = new Factory();

        if (id != null) {
            f = this.factoryRepository.getFactoryById(id);
        }
        f.setCode(code);
        f.setName(name);
        f.set_audit(user);

        f = this.factoryRepository.save(f);

        AjaxResult result = new AjaxResult();
        result.data = f.getId();
        return result;
    }

    @PostMapping("/delete")
    public AjaxResult deleteDepart(@RequestParam("id") Integer id) {

        this.factoryRepository.deleteById(id);

        AjaxResult result = new AjaxResult();

        return result;
    }

}
