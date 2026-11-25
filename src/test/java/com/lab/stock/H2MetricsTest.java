package com.lab.stock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.lab.stock.api.AdminService;
import com.lab.stock.facade.OptimisticFacade;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

/** Grafana 대시보드가 의존하는 메트릭이 /actuator/prometheus 로 실제 노출되는지 확인한다. */
@SpringBootTest
@ActiveProfiles("h2")
@AutoConfigureMockMvc
@AutoConfigureObservability
class H2MetricsTest {

    @Autowired MockMvc mockMvc;
    @Autowired AdminService adminService;
    @Autowired MeterRegistry meterRegistry;
    @Autowired OptimisticFacade optimisticFacade;

    @Test
    void 주문_처리_후_대시보드용_메트릭이_노출된다() throws Exception {
        adminService.reset(10, 1);
        mockMvc.perform(post("/api/orders").param("strategy", "pessimistic")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"productId\":1,\"userId\":1}"))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/orders").param("strategy", "pessimistic")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"productId\":1,\"userId\":1}"))
                .andExpect(status().isConflict());

        String scrape = mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

        assertThat(scrape).contains("stock_order_seconds_bucket", "strategy=\"pessimistic\"",
                "outcome=\"success\"", "outcome=\"duplicate\"",
                "hikaricp_connections_active", "hikaricp_connections_pending",
                "stock_optimistic_retry_total");
    }

    @Test
    void 알_수_없는_전략은_400() throws Exception {
        mockMvc.perform(post("/api/orders").param("strategy", "nope")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"productId\":1,\"userId\":1}"))
                .andExpect(status().isBadRequest());
    }
}
