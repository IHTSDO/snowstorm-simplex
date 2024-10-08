package org.snomed.snowstorm.fhir.config;

import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.interceptor.ResponseHighlighterInterceptor;
import org.snomed.snowstorm.fhir.services.FHIRLoadPackageServlet;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.servlet.MultipartConfigElement;
import java.io.IOException;
import java.nio.file.Files;

@Configuration
public class FHIRRestConfig {

	private static final int MB_IN_BYTES = 1024 * 1024;

	@Bean
	public ServletRegistrationBean<HapiRestfulServlet> hapi() {
		HapiRestfulServlet hapiServlet = new HapiRestfulServlet();

		ServletRegistrationBean<HapiRestfulServlet> servletRegistrationBean = new ServletRegistrationBean<>(hapiServlet, "/fhir/*");
		hapiServlet.setServerName("Snowstorm Simplex FHIR Server");
		hapiServlet.setServerVersion(getClass().getPackage().getImplementationVersion());
		hapiServlet.setDefaultResponseEncoding(EncodingEnum.JSON);

		ResponseHighlighterInterceptor interceptor = new ResponseHighlighterInterceptor();
		hapiServlet.registerInterceptor(interceptor);

		return servletRegistrationBean;
	}

	@Bean
	public ServletRegistrationBean<FHIRLoadPackageServlet> addBundleServlet() throws IOException {
		ServletRegistrationBean<FHIRLoadPackageServlet> registrationBean = new ServletRegistrationBean<>(new FHIRLoadPackageServlet(), "/fhir-admin/load-package");
		registrationBean.setMultipartConfig(
				new MultipartConfigElement(Files.createTempDirectory("fhir-bundle-upload").toFile().getAbsolutePath(), MB_IN_BYTES * 200, MB_IN_BYTES * 200, 0));
		return registrationBean;
	}

}
