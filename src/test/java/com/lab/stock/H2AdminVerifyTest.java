package com.lab.stock;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

/** 부하 실행 후 정합성 검증 API(/admin/verify)가 정상 상태와 깨진 상태를 구분하는지 확인한다. */
@SpringBootTest
@ActiveProfiles("h2")
@AutoConfigureMockMvc
class H2AdminVerifyTest {

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbcTemplate;

    @Test
    void 정상_상태는_consistent_true_재고가_어긋나면_false() throws Exception {
        mockMvc.perform(post("/admin/reset").param("stock", "5").param("products", "2")).andExpect(status().isOk());
        mockMvc.perform(post("/api/orders").param("strategy", "pessimistic")
                .contentType(MediaType.APPLICATION_JSON).content("{\"productId\":1,\"userId\":1}"));

        mockMvc.perform(get("/admin/verify").param("initialStock", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.consistent").value(true))
                .andExpect(jsonPath("$.successOrders").value(1));

        // 주문은 늘었는데 재고를 되돌려 Lost Update 상태를 만든다
        jdbcTemplate.update("UPDATE product SET stock = 5 WHERE id = 1");
        mockMvc.perform(get("/admin/verify").param("initialStock", "5"))
                .andExpect(jsonPath("$.consistent").value(false))
                .andExpect(jsonPath("$.mismatchedProducts").value(1));
    }
}
