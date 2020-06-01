package main

import (
  "github.com/gin-gonic/gin"
  _ "github.com/go-sql-driver/mysql"
  "github.com/jmoiron/sqlx"
  "github.com/kelseyhightower/envconfig"
  log "github.com/sirupsen/logrus"
  swaggerFiles "github.com/swaggo/files"
  ginSwagger "github.com/swaggo/gin-swagger"
  _ "gitlab.com/beehplus/sql-compose/docs"
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

// @title sql-compose-api
// @version 1.0
// @description This is a api for sql-compose.
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host localhost:8080
// @BasePath /
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

  //b, _ := base64.StdEncoding.DecodeString("MjAyMDA1MjY3OQ==")
  //fmt.Println(string(b))

  router := gin.Default()

  url := ginSwagger.URL("http://localhost" +
    s.Port + "/swagger/doc.json")
  router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, url))

  handler := restapi.NewHandler(db)

  router.GET("/doc", handler.GetDocList)
  router.POST("/doc/:uuid", handler.UpdateDoc)
  router.PATCH("/doc", handler.AddDoc)
  router.GET("/doc/:uuid", handler.GetDocDetailByUuid)

  router.GET("/dbconfig", handler.GetDbConfigList)
  router.DELETE("/dbconfig/:uuid/", handler.DeleteDbConfigByUUID)
  router.POST("/dbconfig/:uuid", handler.UpdateDbConfigByUUID)
  router.PATCH("/dbconfig", handler.AddDbConfig)
  router.POST(s.BasePath+"*path", handler.GetResult)

  if err := router.Run(s.Port); err != nil {
    log.Fatal(err)
  }

}
