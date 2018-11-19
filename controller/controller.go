package controllers
 
import (
    "fmt"
    "net/http"
    "github.com/gin-gonic/gin"
)
 
func IndexController(c *gin.Context) {
	c.String(http.StatusOK, "Welcome to log controller!")
}