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
}
