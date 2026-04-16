package mes.app;

import lombok.Getter;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

// 모바일 메뉴 컨트롤러
@Controller
@RequestMapping("/mobile")
public class MobileController {

    @GetMapping("/attendance_submit")
    public String ticketList(Model model) {
        model.addAttribute("currentPage", "attendance_submit");
        return "mobile/attendance_submit"; // "mobile/ticket-list.html"로 매핑
    }

    @GetMapping("/commute_current")
    public String ticketRegister(Model model) {
        model.addAttribute("currentPage", "commute_current");
        return "mobile/commute_current"; // "mobile/ticket-register.html"로 매핑
    }
    @GetMapping("/attendance_current")
    public String kakaoMap2(Model model) {
        model.addAttribute("currentPage", "attendance_current");
        return "mobile/attendance_current";
    }
    @GetMapping("/attendance_statistics")
    public String mlogin(Model model) {
        model.addAttribute("currentPage", "attendance_statistics");
        return "/mobile/attendance_statistics";
    }
    @GetMapping("/user_info")
    public String userInfo(Model model) {
        model.addAttribute("currentPage", "user_info");
        return "/mobile/user_info";
    }
    @GetMapping("/attendance_modify")
    public String modifyAttendance(Model model) {
        model.addAttribute("currentPage", "attendance_modify");
        return "/mobile/attendance_modify";
    }

    @Getter
    public class TempDto{
        private String username;
        private String email;
        private String tel;
        private String address;
        private String nickname;
        private Integer age;

        public TempDto(String username, String email, String phone, String adr, String nick, Integer age){
            this.username = username;
            this.email = email;
            this.tel = phone;
            this.address = adr;
            this.nickname = nick;
            this.age = age;
        }

    }

}