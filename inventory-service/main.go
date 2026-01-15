package main

import (
	"context"
	"inventory-service/handler"
	"inventory-service/initialiser"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	// 1. Initialize Dependencies
	config, cleanup, err := initialiser.InitDependencies()
	if err != nil {
		// Logger might not be initialized if config fails, so we use panic or basic log
		panic(err)
	}
	defer cleanup()

	// 2. Setup Router
	router := gin.Default()
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

	config.Logger.Info("-----------------------Server exiting------------------")
}
