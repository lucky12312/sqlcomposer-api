package restapi

import (
  "fmt"
  "github.com/gin-gonic/gin"
  _ "github.com/go-sql-driver/mysql"
  "github.com/jmoiron/sqlx"
  uuid "github.com/satori/go.uuid"
  log "github.com/sirupsen/logrus"
  "gitlab.com/beehplus/sql-compose/entity"
  "gitlab.com/beehplus/sql-compose/repos"
  "gopkg.in/yaml.v2"
  "net/http"
  "time"
)

type Handler interface {
  GetDocList(c *gin.Context)
  GetDocDetailByUuid(c *gin.Context)
  UpdateDoc(c *gin.Context)
  GetResult(c *gin.Context)

  GetDbConfigList(c *gin.Context)
  AddDbConfig(c *gin.Context)
  DeleteDbConfigByUUID(c *gin.Context)
  UpdateDbConfigByUUID(c *gin.Context)
}

type Service struct {
  Db *sqlx.DB
}

func NewHandler(db *sqlx.DB) *Service {
  return &Service{
    Db: db,
  }
}

func (s *Service) GetDocList(c *gin.Context) {
  var result DocListResult
  var data []*entity.Doc

  err := s.Db.Select(&data, "SELECT uuid,name,path,created_at,updated_at from doc ORDER BY updated_at DESC")
  if err != nil {
    log.Error(err)
  }
  result.Data = data

  err = s.Db.Get(&result, "SELECT COUNT(id) AS total from doc ORDER BY updated_at DESC")
  if err != nil {
    log.Error(err)
  }

  c.JSON(http.StatusOK, result)

}
func (s *Service) GetDocDetailByUuid(c *gin.Context) {

  var doc entity.Doc
  err := s.Db.Get(&doc, "SELECT *  FROM doc WHERE uuid=?", c.Param("uuid"))
  if err != nil {
    log.Error(err)
    c.JSON(http.StatusNotFound, nil)
    return
  }
  c.JSON(http.StatusOK, doc)
}
func (s *Service) UpdateDoc(c *gin.Context) {
  var docEntity entity.Doc

  content := c.PostForm("content")
  var doc repos.SqlApiDoc

  buffer := []byte(content)
  err := yaml.Unmarshal(buffer, &doc)
  if err != nil {
    log.Error(err)
    c.String(http.StatusBadRequest, err.Error())
    return
  }

  params := map[string]interface{}{
    "name":       doc.Info.Name,
    "path":       doc.Info.Path,
    "content":    content,
    "created_at": time.Now().Unix(),
    "updated_at": time.Now().Unix(),
    "uuid":       uuid.NewV4().String(),
  }

  tx := s.Db.MustBegin()
  err = tx.Get(&docEntity, "SELECT uuid from doc where name=?", doc.Info.Name)

  if err != nil {
    ////todo sqlx判断记录为空有更好的方法
    //c.String(http.StatusBadRequest, "the document does not exist")

    _, err = tx.NamedExec(`INSERT into doc (name,path,content,created_at,updated_at,uuid) VALUES (:name,:path,:content,:created_at,:updated_at,:uuid)`,
      params,
    )
    if err != nil {
      log.Warn(err)
      tx.Rollback()
      c.String(http.StatusBadRequest, "insert failed")
      return
    }
    c.String(http.StatusCreated, "insert completed")
    tx.Commit()
    return
  }

  _, err = tx.NamedExec(`UPDATE doc SET path=:path,content=:content,created_at=:created_at,updated_at=:updated_at WHERE name=:name`,
    params, )

  if err != nil {
    log.Warn(err)
    tx.Rollback()
    c.String(http.StatusBadRequest, "update failed")
    return
  }
  tx.Commit()

  c.String(http.StatusCreated, "update completed")
}
func (s *Service) GetResult(c *gin.Context) {
  //get yml by path from db
  path := c.Param("path")

  fmt.Println(path)
  var docEntity entity.Doc
  if err := s.Db.Get(&docEntity, "select * from doc WHERE path=?", path); err != nil {
    log.Error(err)
    c.String(http.StatusNotFound, "this path does not exist")
    return
  }
  //get dsn by dbname
  var doc repos.SqlApiDoc

  buffer := []byte(docEntity.Content)
  err := yaml.Unmarshal(buffer, &doc)
  if err != nil {
    log.Warn(err)
    c.String(http.StatusBadRequest, "please check yaml")
  }

  dbName := doc.Info.Db

  fmt.Println(dbName)

  var dbConfig entity.DataBaseConfig
  err = s.Db.Get(&dbConfig, "SELECT * FROM database_config WHERE name=?", dbName)
  if err != nil {
    c.String(http.StatusBadRequest, "please check dbname")
    return
  }

  dsn := dbConfig.Dsn

  db, err := sqlx.Connect("mysql", dsn)
  if err != nil {
    log.Error(err)
    c.String(http.StatusBadRequest, "database connection error")
    return
  }
  defer db.Close()
  ////todo get sql by yml
  //sqlBuilder, err := repos.NewSqlBuilder(db, []byte(docEntity.Content))
  //if err != nil {
  //  log.Error(err)
  //  c.String(http.StatusBadRequest, err.Error())
  //  return
  //}
  //query, _ := sqlBuilder.BuildQuery()
  //res, _ := query.Queryx(nil)

  c.JSON(http.StatusOK, nil)
}

func (s *Service) AddDbConfig(c *gin.Context) {
  var req AddDbConfigRequest
  c.Bind(&req)
  tx := s.Db.MustBegin()

  _, err := tx.NamedExec("INSERT INTO database_config (uuid,name,dsn,created_at,updated_at) VALUES (:uuid,:name,:dsn,:created_at,:updated_at)",
    map[string]interface{}{
      "uuid":       uuid.NewV4().String(),
      "name":       req.Name,
      "dsn":        req.Dsn,
      "created_at": time.Now().Unix(),
      "updated_at": time.Now().Unix(),
    })
  if err != nil {
    log.Error(err)
    c.String(http.StatusBadRequest, "added failed")
    tx.Rollback()
    return
  }
  tx.Commit()

  c.String(http.StatusCreated, "added successfully")
}
func (s *Service) DeleteDbConfigByUUID(c *gin.Context) {
  uuid := c.Param("uuid")
  s.Db.MustExec("DELETE FROM database_config WHERE uuid=?", uuid)

  c.String(http.StatusCreated, "successfully deleted")
}
func (s *Service) UpdateDbConfigByUUID(c *gin.Context) {
  var req UpdateDbConfigRequest
  if err := c.Bind(&req); err != nil {
    log.Error(err)
    c.String(http.StatusBadRequest, err.Error())
    return
  }
  _, err := s.Db.NamedExec("UPDATE database_config SET name=:name,dsn=:dsn,updated_at=:updated_at WHERE uuid=:uuid",
    map[string]interface{}{
      "name":       req.Name,
      "dsn":        req.Dsn,
      "updated_at": time.Now().Unix(),
      "uuid":       c.Param("uuid"),
    })
  if err != nil {
    log.Error(err)
  }
  c.String(http.StatusOK, "update completed")
}

func (s *Service) GetDbConfigList(c *gin.Context) {
  var list DbConfigList
  err := s.Db.Select(&list.Data, "SELECT * FROM database_config ORDER BY updated_at DESC ")
  if err != nil {
    log.Error(err)
    c.JSON(http.StatusBadRequest, nil)
    return
  }

  err = s.Db.Get(&list, "SELECT COUNT(id) AS total FROM database_config ")
  if err != nil {
    log.Error(err)
    c.JSON(http.StatusBadRequest, nil)
    return
  }

  c.JSON(http.StatusOK, list)
}
