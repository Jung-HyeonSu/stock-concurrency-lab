package com.lab.stock.api;

import java.util.Map;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/admin")
public class AdminController {

    private final AdminService adminService;

    public AdminController(AdminService adminService) {
        this.adminService = adminService;
    }

    /** POST /admin/reset?stock=100&products=1 */
    @PostMapping("/reset")
    public Map<String, Integer> reset(@RequestParam(defaultValue = "100") int stock,
                                      @RequestParam(defaultValue = "1") int products) {
        adminService.reset(stock, products);
        return Map.of("stock", stock, "products", products);
    }

    /** GET /admin/verify?initialStock=100 : 상품별 정합성(재고>=0, 성공 주문 수 == 초기 재고 - 현재 재고) 검증 */
    @GetMapping("/verify")
    public Map<String, Object> verify(@RequestParam int initialStock) {
        return adminService.verify(initialStock);
    }
}
