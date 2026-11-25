package com.lab.stock.api;

import jakarta.validation.constraints.NotNull;

public record OrderRequest(@NotNull Long productId, @NotNull Long userId) {}
