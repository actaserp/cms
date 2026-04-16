package mes.app.dashboard;


import lombok.extern.slf4j.Slf4j;
import mes.app.naverCloud.Enum.NcpMetric;
import mes.app.naverCloud.dto.NetworkChartDto;
import mes.app.naverCloud.service.NcpMonitoringService;
import mes.app.naverCloud.strategy.MonthlyRange;
import mes.app.naverCloud.strategy.RealTimeRange;
import mes.app.traffic.TrafficService;
import mes.app.traffic.dto.TrafficDailyResponse;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/monitoring")
@Slf4j
public class DashBoardMonitoringController {

    @Value("${ncp_api_instanceNo}")
    private String instanceNo;

    @Autowired
    NcpMonitoringService ncpMonitoringService;

    @Autowired
    @Qualifier("asyncExecutor")
    ThreadPoolTaskExecutor asyncExecutors;

    @Autowired
    TrafficService trafficService;


    @GetMapping("/read")
    public AjaxResult GetDataList(//@RequestParam String monthlyStartDate,
                                  @RequestParam String monthlyStartDate2,
                                  @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate trafficStartDate,
                                  @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate trafficEndDate,
                                  @RequestParam(defaultValue = "1") int pageNumber,
                                  @RequestParam(defaultValue = "10") int pageSize
                                  ){

        // CPU와 RAM 메트릭을 '실시간(30분)' 정책으로 묶어서 단일 시간 요청
        CompletableFuture<Map<String, Double>> resourceSummary = CompletableFuture.supplyAsync(() ->
                        ncpMonitoringService.fetchAverages(
                                List.of(NcpMetric.avg_cpu_used_rto, NcpMetric.mem_usert),
                                instanceNo,
                                new RealTimeRange()
                        ), asyncExecutors
                );

        //네트워크 대시보드 데이터
        CompletableFuture<NetworkChartDto> trafficHistory = CompletableFuture.supplyAsync(() ->
                ncpMonitoringService.fetchTrafficHistory(
                        List.of(NcpMetric.avg_snd_bps, NcpMetric.avg_rcv_bps), "127900112"
                        ,new MonthlyRange()
                ), asyncExecutors
        );



        //월별 가입현황 대시보드 데이터

        /*CompletableFuture<List<Map<String, Object>>> montlyList = CompletableFuture.supplyAsync(() ->
                ncpMonitoringService.getMontlyRegisterList(monthlyStartDate, pageNumber, pageSize)
                ,asyncExecutors
        );*/

        //CompletableFuture<List<Map<String, Object>>> montlyList = null

        //api 콜 횟수 (고객사별) 대시보드 데이터

        CompletableFuture<List<Map<String, Object>>> apiCntListBySpjangcd = CompletableFuture.supplyAsync(() ->
                ncpMonitoringService.getApiCntListBySpjangcd(pageNumber, pageSize, monthlyStartDate2)
                ,asyncExecutors
        );

        // 트래픽 수집 데이터 (날짜 범위 없으면 당월 기본값)
        LocalDate start = trafficStartDate != null ? trafficStartDate : LocalDate.now().withDayOfMonth(1);
        LocalDate end   = trafficEndDate   != null ? trafficEndDate   : LocalDate.now();

        CompletableFuture<List<TrafficDailyResponse>> trafficDailyList = CompletableFuture.supplyAsync(() ->
                trafficService.getTrafficByDateRange(start, end), asyncExecutors
        );


        Map<String, Object> dataList = new HashMap<>();
        try{
            //dataList.put("resource", null);
            dataList.put("resource", resourceSummary.join());
            //dataList.put("traffic", null);
            dataList.put("traffic", trafficHistory.join());
            //dataList.put("monthly", montlyList.join());
            dataList.put("apiCntList", apiCntListBySpjangcd.join());
            dataList.put("trafficDaily", trafficDailyList.join());
        }catch (Exception e){
            log.error("데이터 조립 중 에러 발생", e);
        }
        return AjaxResult.success(null, dataList);
    }

    //월별 가입현황 (페이징)
    //@GetMapping("/monthly_read")
    @GetMapping("/pages/monthly")
    public AjaxResult getMontlyList(@RequestParam String monthlyStartDate,
                                    @RequestParam(defaultValue = "1") int pageNumber,
                                    @RequestParam(defaultValue = "10") int pageSize
                                    ){
        List<Map<String, Object>> data = ncpMonitoringService.getMontlyRegisterList(
                monthlyStartDate, pageNumber, pageSize
        );


        return AjaxResult.success(null, data);
    }

    //실시간 사용량 및 정산현황 (페이징)
    //@GetMapping("/api_count_list")
    @GetMapping("/pages/usage")
    public AjaxResult getApiCntList(
            @RequestParam String monthlyStartDate2,
            @RequestParam(defaultValue = "1") int pageNumber,
            @RequestParam(defaultValue = "10") int pageSize
    ){
        List<Map<String, Object>> data = ncpMonitoringService.getApiCntListBySpjangcd(pageNumber, pageSize, monthlyStartDate2);

        return AjaxResult.success(null, data);
    }




    @GetMapping("/local_cache/save")
    public AjaxResult localCacheSetRDB(){

        ncpMonitoringService.redisDataSync();
        //ncpMonitoringService.syncCacheToDb();

        return AjaxResult.success(null, null);
    }
}
