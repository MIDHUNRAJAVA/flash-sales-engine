package main

import (
	"context"
	"inventory-service/actions"
	"inventory-service/handler"
	"inventory-service/initialiser"
	"inventory-service/tracing"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

func main() {
	// 1. Initialize Dependencies
	config, cleanup, err := initialiser.InitDependencies()
	if err != nil {
		// Logger might not be initialized if config fails, so we use panic or basic log
		panic(err)
	}
	defer cleanup()

	// 1b. Tracing: OTLP/gRPC exporter + global TracerProvider. A dead collector
	// must not crash us, so a failed init only downgrades to a warning.
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "inventory"
	}
	shutdownTracing, err := tracing.Init(context.Background(), serviceName,
		os.Getenv("SERVICE_VERSION"), os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if err != nil {
		config.Logger.Warn("Tracing init failed, continuing without export", "error", err)
	}

	// 2. Background loops: reservation-expiry reaper + stock gauge poller
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()
	go actions.RunReaper(bgCtx, config)
	go actions.RunStockPoller(bgCtx, config)

	// 3. Setup Router
	router := gin.Default()
	router.Use(otelgin.Middleware("inventory"))
	router.GET("/health", handler.HandleHealth(config))
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	router.POST("/buy", handler.HandleBuy(config))
	router.POST("/seed", handler.HandleSeed(config))

	// 3. Run Server
	srv := &http.Server{
		Addr:    ":" + config.Server.Port,
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			config.Logger.Error("Failed to start server", "error", err)
			os.Exit(1)
		}
	}()
	config.Logger.Info("Inventory Service running", "port", config.Server.Port)

	// Wait for interrupt signal, to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	config.Logger.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		config.Logger.Error("Server forced to shutdown", "error", err)
	}

	if shutdownTracing != nil {
		if err := shutdownTracing(ctx); err != nil {
			config.Logger.Error("Tracing shutdown failed", "error", err)
		}
	}

	config.Logger.Info("-----------------------Server exiting------------------")
}
