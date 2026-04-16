package mes.app.weather.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Service
public class WeatherService {

	@Value("${weather.api.endpoint}")
	private String apiEndpoint;

	@Value("${api.key}")
	private String apiKey;

	private final RestTemplate restTemplate;

	@Autowired
	public WeatherService(RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
	}

	@Value("${Geocoder.Key}")
	private String geocoderKey;

	@Autowired
	TB_xusersService xusersService;

/*	❍단기예보
- Base_time : 0200, 0500, 0800, 1100, 1400, 1700, 2000, 2300 (1일 8회)
- API 제공 시간(~이후) : 02:10, 05:10, 08:10, 11:10, 14:10, 17:10, 20:10, 23:10*/

	//TODO : 시간 오류 추후 수정 (오전 9시일 경우 0500 데이터로 조회됨)
	private String determineBaseTime(LocalDateTime now) {
		int hour = now.getHour();
		int minute = now.getMinute();

		if (minute >= 10) {
			if (hour >= 23) return "2300";
			else if (hour >= 20) return "2000";
			else if (hour >= 17) return "1700";
			else if (hour >= 14) return "1400";
			else if (hour >= 11) return "1100";
			else if (hour >= 8) return "0800";
			else if (hour >= 5) return "0500";
			else return "0200";
		} else {
			if (hour >= 23) return "2000";
			else if (hour >= 20) return "1700";
			else if (hour >= 17) return "1400";
			else if (hour >= 14) return "1100";
			else if (hour >= 11) return "0800";
			else if (hour >= 8) return "0500";
			else return "0200";
		}
	}

	private String determineUltraSrtBaseTime(LocalDateTime now) {
		int minute = now.getMinute();
		int roundedMinute = (minute / 10) * 10;
		LocalDateTime baseTime = now.withMinute(roundedMinute).withSecond(0).withNano(0);

		// 데이터 제공 지연 보정: 40분 이전이면 한 타임 전으로
		if (minute < 40) {
			baseTime = baseTime.minusMinutes(10);
		}

		return baseTime.format(DateTimeFormatter.ofPattern("HHmm"));
	}

	// getWeatherData 메서드 내에서 fetchWeatherData 호출 시 latitude와 longitude를 인자로 전달
	public ResponseEntity<?> getWeatherData(String userId){

		// 사용자 주소를 가져오기(사업장 주소 사용)
			String address = xusersService.getUserAddress(userId);
		if (address == null || address.isEmpty()) {
			return ResponseEntity.badRequest().body("주소가 유효하지 않습니다.");
		}

		LocalDateTime now = LocalDateTime.now();
		String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
		String ultraSrtBaseTime = determineUltraSrtBaseTime(now); // 초단기실황용
		String forecastBaseTime = determineBaseTime(now);         // 단기예보용

		// 사용자 주소를 통해 좌표를 얻기
		double[] coordinates = getCoordinates(address, geocoderKey);
		double latitude = coordinates[0]; // 위도
		double longitude = coordinates[1]; // 경도
		/*System.out.println("Latitude (위도): " + coordinates[0]);
		System.out.println("Longitude (경도): " + coordinates[1]);*/

		// 초단기실황 조회
		ResponseEntity<?> currentWeather = fetchWeatherData("/getUltraSrtNcst", date, ultraSrtBaseTime, "current", latitude, longitude);
		// 단기예보 조회
		ResponseEntity<?> forecastData = fetchWeatherData("/getVilageFcst", date, forecastBaseTime, "forecast", latitude, longitude);

		return combineData(currentWeather, forecastData, address);
	}

	//위도(latitude)와 경도(longitude)**를 **기상청 격자 좌표(nx, ny)**로 변환
	public class CoordinateConverter {
		// 기상청 격자 변환 기준
		private static final double RE = 6371.00877; // 지구 반경(km)
		private static final double GRID = 5.0; // 격자 간격(km)
		private static final double SLAT1 = 30.0; // 표준 위도 1(도)
		private static final double SLAT2 = 60.0; // 표준 위도 2(도)
		private static final double OLON = 126.0; // 기준점 경도(도)
		private static final double OLAT = 38.0; // 기준점 위도(도)
		private static final double XO = 43; // 기준점 X좌표(GRID)
		private static final double YO = 136; // 기준점 Y좌표(GRID)

		public static Map<String, Integer> convertToGrid(double latitude, double longitude) {
			double DEGRAD = Math.PI / 180.0;
			double RADDEG = 180.0 / Math.PI;

			double re = RE / GRID;
			double slat1 = SLAT1 * DEGRAD;
			double slat2 = SLAT2 * DEGRAD;
			double olon = OLON * DEGRAD;
			double olat = OLAT * DEGRAD;

			double sn = Math.tan(Math.PI * 0.25 + slat2 * 0.5) / Math.tan(Math.PI * 0.25 + slat1 * 0.5);
			sn = Math.log(Math.cos(slat1) / Math.cos(slat2)) / Math.log(sn);
			double sf = Math.tan(Math.PI * 0.25 + slat1 * 0.5);
			sf = Math.pow(sf, sn) * Math.cos(slat1) / sn;
			double ro = Math.tan(Math.PI * 0.25 + olat * 0.5);
			ro = re * sf / Math.pow(ro, sn);
			double ra = Math.tan(Math.PI * 0.25 + latitude * DEGRAD * 0.5);
			ra = re * sf / Math.pow(ra, sn);
			double theta = longitude * DEGRAD - olon;
			if (theta > Math.PI) theta -= 2.0 * Math.PI;
			if (theta < -Math.PI) theta += 2.0 * Math.PI;
			theta *= sn;

			int nx = (int) Math.floor(ra * Math.sin(theta) + XO + 0.5);
			int ny = (int) Math.floor(ro - ra * Math.cos(theta) + YO + 0.5);

			Map<String, Integer> grid = new HashMap<>();
			grid.put("nx", nx);
			grid.put("ny", ny);
			return grid;
		}
	}

	private ResponseEntity<?> fetchWeatherData(String servicePath, String date, String time, String dataSource, double latitude, double longitude) {
		try {
			// 위도와 경도를 격자 좌표로 변환
			Map<String, Integer> gridCoordinates = CoordinateConverter.convertToGrid(latitude, longitude);
			int nx = gridCoordinates.get("nx");
			int ny = gridCoordinates.get("ny");

			// 현재 시간으로 데이터 요청
			URI uri = new URI(apiEndpoint + servicePath +
					"?serviceKey=" + apiKey +
					"&pageNo=1" +
					"&numOfRows=100" +
					"&dataType=json" +
					"&base_date=" + date +
					"&base_time=" + time +
					"&nx=" + nx +
					"&ny=" + ny
			);

//			System.out.println("날씨 uri (현재 시간): " + uri);

			String response = restTemplate.getForObject(uri, String.class);
			ResponseEntity<?> parsedResponse = parseWeatherData(response, dataSource);

			if (parsedResponse.getStatusCode().is2xxSuccessful()) {
				return parsedResponse;
			}

			// 데이터가 없는 경우, 이전 시간으로 조정하여 재요청
			System.out.println("현재 시간의 데이터가 없어 이전 시간으로 조정 중...");

			// 이전 시간으로 조정
			LocalDateTime newDateTime = LocalDateTime.parse(date + time, DateTimeFormatter.ofPattern("yyyyMMddHHmm"));

			if ("current".equals(dataSource)) {
				newDateTime = newDateTime.minusMinutes(10); // 초단기실황: 10분 전
				time = newDateTime.format(DateTimeFormatter.ofPattern("HHmm"));
			} else {
				newDateTime = newDateTime.minusHours(3); // 단기예보: 3시간 전
				time = newDateTime.format(DateTimeFormatter.ofPattern("HH00"));
			}

			date = newDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

			uri = new URI(apiEndpoint + servicePath +
					"?serviceKey=" + apiKey +
					"&pageNo=1" +
					"&numOfRows=100" +
					"&dataType=json" +
					"&base_date=" + date +
					"&base_time=" + time +
					"&nx=" + nx +
					"&ny=" + ny
			);

			//System.out.println("날씨 uri (이전 시간): " + uri);

			response = restTemplate.getForObject(uri, String.class);
			parsedResponse = parseWeatherData(response, dataSource);

			return parsedResponse;

		} catch (URISyntaxException e) {
			e.printStackTrace();
			return ResponseEntity.badRequest().body("URI syntax error");
		}
	}

	private ResponseEntity<?> parseWeatherData(String response, String dataSource) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode root = mapper.readTree(response);
			JsonNode items = root.path("response").path("body").path("items").path("item");

			Map<String, String> result = new HashMap<>();
			for (JsonNode item : items) {
				String category = item.path("category").asText();
				String value;
				if ("forecast".equals(dataSource)) {
					value = item.path("fcstValue").asText();  // 단기예보에서는 fcstValue 사용
				} else {
					value = item.path("obsrValue").asText();  // 초단기실황에서는 obsrValue 사용
				}
				result.put(category, value);
//				System.out.println("Parsed " + category + ": " + value);
			}

//			System.out.println("최종 데이터 : " + result);
			return ResponseEntity.ok(result);
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.badRequest().body("Failed to parse weather data");
		}
	}

	private ResponseEntity<?> combineData(ResponseEntity<?> weatherData, ResponseEntity<?> forecastData, String address) {
		Object body1 = weatherData.getBody();
		Object body2 = forecastData.getBody();

		if (!(body1 instanceof Map) || !(body2 instanceof Map)) {
			return ResponseEntity.badRequest().body("날씨 데이터 파싱 실패 또는 응답 오류 발생");
		}

		Map<String, String> weatherResult = (Map<String, String>) body1;     // 실황 (기온, 습도, 풍속 등)
		Map<String, String> forecastResult = (Map<String, String>) body2;    // 예보 (POP, SKY, PTY 등)

		// forecastData에서 필요한 항목만 병합 (POP, SKY, PTY만)
		for (Map.Entry<String, String> entry : forecastResult.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();

			if (key.equals("POP") || key.equals("SKY") || key.equals("PTY")) {
				weatherResult.put(key, value);
			}
		}

		// 지역 주소 추가
		weatherResult.put("address", address);

		return ResponseEntity.ok(weatherResult);
	}

	// 좌표를 얻기 위한 메서드
	private double[] getCoordinates(String address, String apikey) {
		String epsg = "epsg:4326";
		StringBuilder sb = new StringBuilder("https://api.vworld.kr/req/address");
		sb.append("?service=address");
		sb.append("&request=getCoord");
		sb.append("&version=2.0");
		sb.append("&crs=").append(epsg);
		sb.append("&key=").append(apikey);
		sb.append("&refine=true");
		sb.append("&simple=false");
		sb.append("&format=json");
		sb.append("&type=ROAD");
		sb.append("&address=").append(URLEncoder.encode(address, StandardCharsets.UTF_8));

		try {
			URL url = new URL(sb.toString());
			BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8));
			ObjectMapper mapper = new ObjectMapper();
			JsonNode root = mapper.readTree(reader);

			// 응답 데이터 검증
			JsonNode result = root.path("response").path("result");
			if (result.isMissingNode()) {
				throw new RuntimeException("좌표 정보를 찾을 수 없습니다.");
			}

			JsonNode point = result.path("point");
			double x = point.path("x").asDouble(); // 경도
			double y = point.path("y").asDouble(); // 위도

			// 주소 정보 확인 및 반환
			JsonNode refined = root.path("response").path("refined");
			String refinedText = refined.path("text").asText(); // 정제된 주소
			String detail = refined.path("structure").path("detail").asText(); // 상세주소

			/*System.out.println("정제된 주소: " + refinedText);
			System.out.println("상세 주소: " + detail);*/

			return new double[]{y, x}; // [위도, 경도] 반환
		} catch (IOException e) {
			throw new RuntimeException("좌표를 가져오는 데 실패했습니다: " + e.getMessage(), e);
		}
	}

}