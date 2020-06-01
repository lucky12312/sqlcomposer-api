package restapi

import (
  "fmt"
  "github.com/gin-gonic/gin"
  _ "github.com/go-sql-driver/mysql"
  "github.com/jmoiron/sqlx"
  uuid "github.com/satori/go.uuid"
  log "github.com/sirupsen/logrus"
  "github.com/wangxb07/sqlcomposer"
  "gitlab.com/beehplus/sql-compose/entity"
  "gitlab.com/beehplus/sql-compose/repos"
  "gopkg.in/yaml.v2"
  "net/http"
  "time"
)

type Handler interface {
  GetDocList(c *gin.Context)
  GetDocDetailByUuid(c *gin.Context)
  AddDoc(c *gin.Context)
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

// @Summary 添加新的文档
// @Tags 文档
// @version 1.0
// @Param content formData string true "文档内容"
// @Success 201 {string} string	""insert completed""
// @Failure 400 {object} Error "deserialize yaml failed"
// @Router /doc [patch]
func (s *Service) AddDoc(c *gin.Context) {
  content := c.PostForm("content")
  fmt.Println(content)

  var doc repos.SqlApiDoc

  buffer := []byte(content)
  err := yaml.Unmarshal(buffer, &doc)
  if err != nil {
    log.Error(err)
    c.JSON(http.StatusBadRequest, Error{
      Code:    40002,
      Message: "deserialize yaml failed",
    })
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

  ////todo sqlx判断记录为空有更好的方法
  //c.String(http.StatusBadRequest, "the document does not exist")

  _, err = s.Db.NamedExec(`INSERT into doc (name,path,content,created_at,updated_at,uuid) VALUES (:name,:path,:content,:created_at,:updated_at,:uuid)`,
    params,
  )
  if err != nil {
    log.Warn(err)
    c.JSON(http.StatusBadRequest, Error{
      Code:    40001,
      Message: "insert failed,maybe the name is duplicated",
    })
    return
  }
  c.String(http.StatusCreated, "insert completed")
}

// @Summary 获取文档列表
// @Uuid xxx123
// @Tags 文档
// @version 1.0
// @Success 200 {object} DocListResult
// @Router /doc [get]
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

// @Summary 获取文档详情
// @Tags 文档
// @version 1.0
// @Param uuid path string true "uuid"
// @Success 200 {object} entity.Doc
// @Router /doc/{uuid} [get]
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

// @Summary 更新文档
// @Tags 文档
// @version 1.0
// @Param uuid path string true "uuid"
// @Param content formData string true "content"
// @Success 201 {string} string	"update completed"
// @Failure 400 {object} Error "error"
// @Router /doc/{uuid} [post]
func (s *Service) UpdateDoc(c *gin.Context) {
  var docEntity entity.Doc

  content := c.PostForm("content")
  var doc repos.SqlApiDoc

  buffer := []byte(content)
  err := yaml.Unmarshal(buffer, &doc)
  if err != nil {
    log.Error(err)
    c.JSON(http.StatusBadRequest, Error{
      Code:    40002,
      Message: err.Error(),
    })
    return
  }

  params := map[string]interface{}{
    "name":       doc.Info.Name,
    "path":       doc.Info.Path,
    "content":    content,
    "created_at": time.Now().Unix(),
    "updated_at": time.Now().Unix(),
    "uuid":       c.Param("uuid"),
  }

  tx := s.Db.MustBegin()
  err = tx.Get(&docEntity, "SELECT uuid from doc where uuid=?", c.Param("uuid"))

  if err != nil {
    c.JSON(http.StatusBadRequest, Error{
      Code:    40003,
      Message: "This document does not exist",
    })
    return
  }

  _, err = tx.NamedExec(`UPDATE doc SET name=:name,path=:path,content=:content,created_at=:created_at,updated_at=:updated_at WHERE uuid=:uuid`,
    params, )

  if err != nil {
    log.Warn(err)
    tx.Rollback()
    c.JSON(http.StatusBadRequest, Error{
      Code:    40004,
      Message: "update failed",
    })
    return
  }
  tx.Commit()

  c.String(http.StatusCreated, "update completed")
}

// @Summary 获取查询结果
// @Tags 接口
// @version 1.0
// @Param path path string true "path"
// @Success 200 {string} string	"json"
// @Failure 400 {object} Error "error"
// @Failure 404 {object} Error "not found"
// @Router /{path} [get]
func (s *Service) GetResult(c *gin.Context) {
  //get yml by path from db
  path := c.Param("path")

  var docEntity entity.Doc
  if err := s.Db.Get(&docEntity, "select * from doc WHERE path=?", path); err != nil {
    log.Error(err)
    c.JSON(http.StatusNotFound, Error{
      Code:    40005,
      Message: "this path does not exist",
    })
    return
  }

  //get filter params
  var req GetResultRequest
  if err := c.BindJSON(&req); err != nil {
    log.Error(err)
    c.JSON(http.StatusBadRequest, Error{
      Code:    40006,
      Message: "filter params error",
    })
    return
  }

  var reposFilters []sqlcomposer.Filter
  for _, filter := range req.Filters {
    reposFilter := sqlcomposer.Filter{
      Val:  filter.Key,
      Op:   repos.Contains,
      Attr: filter.Val,
    }
    reposFilters = append(reposFilters, reposFilter)
  }

  fmt.Println(reposFilters)

  where, err := sqlcomposer.WhereAnd(&reposFilters)
  //get dsn by dbname
  var doc repos.SqlApiDoc

  buffer := []byte(docEntity.Content)
  err = yaml.Unmarshal(buffer, &doc)
  if err != nil {
    log.Warn(err)
    c.JSON(http.StatusBadRequest, Error{
      Code:    40007,
      Message: err.Error(),
    })
  }

  dbName := doc.Info.Db

  var dbConfig entity.DataBaseConfig
  err = s.Db.Get(&dbConfig, "SELECT * FROM database_config WHERE name=?", dbName)
  if err != nil {
    c.JSON(http.StatusBadRequest, Error{
      Code:    40008,
      Message: "please check dbname",
    })
    return
  }

  dsn := dbConfig.Dsn

  db, err := sqlx.Connect("mysql", dsn)
  if err != nil {
    log.Error(err)
    c.JSON(http.StatusBadRequest, Error{
      Code:    40009,
      Message: "database connection error",
    })
    return
  }
  defer db.Close()



  sqlBuilder, err := sqlcomposer.NewSqlBuilder(db,[]byte(docEntity.Content))
  if err != nil {
    log.Error(err)
    c.JSON(http.StatusBadRequest, Error{
      Code:    40010,
      Message: err.Error(),
    })
    return
  }

  if err != nil {
    log.Fatal(err)
  }

  q, a, err:= sqlBuilder.AndConditions(&where).Limit(0,10).Rebind("test")

  fmt.Println(q)

  rows, err := db.Queryx(q, a...)

  row := make(map[string]interface{})
  err = rows.MapScan(row)

  if err != nil {
    log.Fatal(err)
  }

  sqlBuilder.RowConvert(&row)

  fmt.Println(row)

  c.JSON(http.StatusOK, row)
}

// @Summary 添加数据库配置
// @Tags 数据库配置
// @version 1.0
// @Param params body AddDbConfigRequest true "DbConfig"
// @Success 201 {string} string	"json"
// @Failure 400 {object} Error "error"
// @Router /dbconfig [patch]
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
    c.JSON(http.StatusBadRequest, Error{
      Code:    40001,
      Message: "added failed",
    })
    tx.Rollback()
    return
  }
  tx.Commit()

  c.String(http.StatusCreated, "added successfully")
}

// @Summary 删除数据库配置
// @Tags 数据库配置
// @version 1.0
// @Param uuid path string true "uuid"
// @Success 201 {string} string	"json"
// @Failure 400 {object} Error "error"
// @Router /dbconfig/{uuid} [delete]
func (s *Service) DeleteDbConfigByUUID(c *gin.Context) {
  uuid := c.Param("uuid")
  s.Db.MustExec("DELETE FROM database_config WHERE uuid=?", uuid)

  c.String(http.StatusCreated, "successfully deleted")
}

// @Summary 更新数据库配置
// @Tags 数据库配置
// @version 1.0
// @Param params body AddDbConfigRequest true "DbConfig"
// @Param uuid path string true "uuid"
// @Success 201 {string} string	"更新成功"
// @Failure 400 {object} Error "error"
// @Router /dbconfig/{uuid} [post]
func (s *Service) UpdateDbConfigByUUID(c *gin.Context) {
  var req UpdateDbConfigRequest
  if err := c.Bind(&req); err != nil {
    log.Error(err)
    c.JSON(http.StatusBadRequest, Error{
      Code:    40006,
      Message: err.Error(),
    })
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

// @Summary 数据库配置列表
// @Tags 数据库配置
// @version 1.0
// @Success 200 {object} DbConfigList
// @Router /dbconfig [get]
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
