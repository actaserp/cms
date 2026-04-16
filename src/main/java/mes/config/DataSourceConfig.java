package mes.config;

import java.util.HashMap;
import java.util.List;

import javax.sql.DataSource;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.app.interceptor.TenantSqlInspector;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Slf4j
@EnableJpaRepositories(basePackages = "mes.domain.repository",  entityManagerFactoryRef = "entityManagerFactory", transactionManagerRef = "transactionManagerCustom")
@Configuration
public class DataSourceConfig {

	@Bean("jdbcTemplate")
	JdbcTemplate jdbcTemplate(DataSource dataSource) {
		return new JdbcTemplate(dataSource) {
			// 1. 단일 조회 가로채기
			@Override
			public <T> T queryForObject(String sql, RowMapper<T> rowMapper) {
				validateTenantIsolation(sql);
				return super.queryForObject(sql, rowMapper);
			}

			// 2. 리스트 조회 가로채기
			@Override
			public <T> List<T> query(String sql, RowMapper<T> rowMapper) {
				validateTenantIsolation(sql);
				return super.query(sql, rowMapper);
			}

			// 3. 파라미터가 포함된 조회 가로채기
			@Override
			public <T> List<T> query(String sql, Object[] args, RowMapper<T> rowMapper) {
				validateTenantIsolation(sql);
				return super.query(sql, args, rowMapper);
			}

			// 4. 수정/삭제/삽입(DML) 가로채기
			@Override
			public int update(String sql, Object... args) {
				validateTenantIsolation(sql);
				return super.update(sql, args);
			}

			// 5. 일반 실행 가로채기
			@Override
			public void execute(String sql) {
				validateTenantIsolation(sql);
				super.execute(sql);
			}
		};
	}

	@Bean("namedParameterJdbcTemplate")
	@Primary
	public NamedParameterJdbcTemplate namedParameterJdbcTemplate(@Qualifier("jdbcTemplate") JdbcTemplate jdbcTemplate) {
		// 반드시 @Qualifier로 위에서 만든 보안 jdbcTemplate을 주입받아야 합니다.
		return new NamedParameterJdbcTemplate(jdbcTemplate);
	}

	// 공통 검증 로직
	private void validateTenantIsolation(String sql) {
		String tenantId = TenantContext.get();
		String lowSql = sql.toLowerCase();

		// [예외 1] 로그인/인증 단계 및 공통 테이블(사용자, 메뉴로그)은 검증 패스
		if (lowSql.contains("tb_user") || lowSql.contains("menu_use_log") || lowSql.contains("sys_common_code")) {
			return;
		}

		// [예외 2] 인증 정보가 아예 없는 경우 (인터셉터를 거치지 않은 비정상 접근)
		if (tenantId == null) {
			throw new SecurityException("인증된 테넌트 정보가 없습니다. DB 접근이 차단되었습니다.");
		}

		// [핵심] 일반 엔티티 조회 시 spjangcd 조건이 없으면 실행 차단
		if (!lowSql.contains("spjangcd")) {
			log.error("!!! 격리 정책 위반 감지 !!! 실행 쿼리: {}", sql);
			throw new SecurityException("데이터 격리 정책 위반: 쿼리에 spjangcd 조건이 누락되었습니다.");
		}
	}
	
	@ConfigurationProperties(prefix="spring.datasource.hikari")
	@Bean(name="dataSource")	
	DataSource dataSource() {
		return DataSourceBuilder.create().build();
	}	
	
	@Bean(name="entityManagerFactory")
	LocalContainerEntityManagerFactoryBean entityManagerFactory(){
		LocalContainerEntityManagerFactoryBean emf = new LocalContainerEntityManagerFactoryBean();
		emf.setDataSource(this.dataSource());
		emf.setPackagesToScan(new String[]{"mes.domain.entity"});
		HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
		emf.setJpaVendorAdapter(vendorAdapter);
		HashMap<String, Object> properties =new HashMap<>();

		properties.put("hibernate.session_factory.statement_inspector", new TenantSqlInspector());
		properties.put("hibernate.ddl-auto", "validate");
		properties.put("hibernate.format_sql",true);
		properties.put("hibernate.show-sql",true);
		properties.put("hibernate.dialect","org.hibernate.dialect.PostgreSQLDialect");
		//properties.put("hibernate.storage_engine", property.getStorage_engine());
		emf.setJpaPropertyMap(properties);	
		return emf;
    } 
	
	@Bean
	@Primary
	PlatformTransactionManager transactionManagerCustom(){
		JpaTransactionManager transactionManager =new JpaTransactionManager();
		transactionManager.setEntityManagerFactory(this.entityManagerFactory().getObject()); 
		return transactionManager;	
	}
	
	@Bean  
	TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {  
	    return new TransactionTemplate(transactionManager);  
	}	
	
	
	@Bean
	SqlSessionFactory sqlSessionFactory(DataSource dataSource) throws Exception{
		final SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
		sessionFactory.setDataSource(dataSource);
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		sessionFactory.setMapperLocations(resolver.getResources("mapper/*.xml")); 	//mapper 파일 로드
		return sessionFactory.getObject();
	}
	
	@Bean
	SqlSessionTemplate sqlSessionTemplate(SqlSessionFactory sqlSessionFactory) throws Exception{
		final SqlSessionTemplate sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory);
		return sqlSessionTemplate;
	}


}
