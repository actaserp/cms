package mes.app.traffic;

import mes.app.Scheduler.SchedulerService.NginxTrafficService;
import mes.app.traffic.dto.TrafficDailyResponse;
import mes.app.traffic.dto.TrafficUploadResponse;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDate;
import java.util.List;

@RestController
@RequestMapping("/api/traffic")
public class TrafficController {

    @Autowired
    TrafficService trafficService;

    @PostMapping("/upload")
    public AjaxResult uploadLogFile(@RequestParam MultipartFile file,
                                    @RequestParam String service,
                                    @RequestParam("date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)LocalDate date
                                    ){
        TrafficUploadResponse response = trafficService.collectFromUploadedFile(file, service, date);

        return AjaxResult.success(null, response);

    }

    @GetMapping("/range")
    public AjaxResult getTrafficByRange(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate,
            @RequestParam(defaultValue = "false") boolean groupByService
    ){
        List<TrafficDailyResponse> result = groupByService
                ? trafficService.getTrafficGroupedByService(startDate, endDate)
                : trafficService.getTrafficByDateRange(startDate, endDate);

        return AjaxResult.success(null, result);
    }
}
