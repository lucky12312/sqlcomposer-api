package main

import (
  "github.com/gin-gonic/gin"
  _ "github.com/go-sql-driver/mysql"
  "github.com/jmoiron/sqlx"
  "github.com/kelseyhightower/envconfig"
  log "github.com/sirupsen/logrus"
  "gitlab.com/beehplus/sql-compose/restapi"
  "time"
)

//env
type Specification struct {
  Debug      bool
  Port       string
  BasePath   string
  Dsn        string
  User       string
  Rate       float32
  Timeout    time.Duration
  ColorCodes map[string]int
}

func main() {

  var s Specification
  if err := envconfig.Process("sqlcompose", &s); err != nil {
    log.Fatal(err)
  }

  log.Info(s)

  //init db
  db, err := sqlx.Connect("mysql", s.Dsn)
  if err != nil {
    log.Fatal(err)
  }
  defer db.Close()

  router := gin.Default()

  handler := restapi.NewHandler(db)

  router.GET("/doc", handler.GetDocList)
  router.POST("/doc", handler.UpdateDoc)
  router.GET("/doc/:uuid", handler.GetDocDetailByUuid)

  router.GET("/dbconfig", handler.GetDbConfigList)
  router.DELETE("/dbconfig/:uuid/", handler.DeleteDbConfigByUUID)
  router.POST("/dbconfig/:uuid", handler.UpdateDbConfigByUUID)
  router.PATCH("/dbconfig", handler.AddDbConfig)
  router.GET(s.BasePath+"*path", handler.GetResult)

  if err := router.Run(s.Port); err != nil {
    log.Fatal(err)
  }

}
