package restapi

import (
	"database/sql"
	"fmt"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"github.com/wangxb07/sqlcomposer"
	"gitlab.com/beehplus/sql-compose/entity"
	"gopkg.in/yaml.v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Handler interface {
	GetDocList(c *gin.Context)
	GetDocDetailByUuid(c *gin.Context)
	AddDoc(c *gin.Context)
	UpdateDoc(c *gin.Context)
	DeleteDoc(c *gin.Context)
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

// @Summary 删除文档
// @Tags 文档
// @version 1.0
// @Param uuid path string true "uuid"
// @Success 201 {string} string	""delete completed""
// @Failure 400 {object} Error "error"
// @Router /doc/{uuid} [delete]
func (s *Service) DeleteDoc(c *gin.Context) {
	uuid := c.Param("uuid")
	fmt.Println(uuid)
	s.Db.MustExec("DELETE FROM doc WHERE uuid=?", uuid)
	c.String(http.StatusCreated, "successfully deleted")
}

// @Summary 添加新的文档
// @Tags 文档
// @version 1.0
// @Param content formData string true "文档内容"
// @Param path formData string true "接口路径"
// @Success 201 {string} string	""insert completed""
// @Failure 400 {object} Error "deserialize yaml failed"
// @Router /doc [patch]
func (s *Service) AddDoc(c *gin.Context) {
	content := c.PostForm("content")
	path := c.PostForm("path")
	var doc sqlcomposer.SqlApiDoc

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
		"path":       path,
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
// @Param path formData string true "path"
// @Success 201 {string} string	"update completed"
// @Failure 400 {object} Error "error"
// @Router /doc/{uuid} [post]
func (s *Service) UpdateDoc(c *gin.Context) {
	var docEntity entity.Doc

	content := c.PostForm("content")
	path := c.PostForm("path")

	var doc sqlcomposer.SqlApiDoc

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
		"path":       path,
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
// @Param dbname query string true "dbname"
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

	fmt.Println(req)

	var custFilters []sqlcomposer.Filter
	for _, filter := range req.Filters {
		custFilter := sqlcomposer.Filter{
			Val:  filter.Val,
			Op:   filter.Op,
			Attr: filter.Attr,
		}
		custFilters = append(custFilters, custFilter)
	}

	//whereAnd, err := sqlcomposer.WhereAnd(&custFilters)
	//get dsn by dbname
	var doc sqlcomposer.SqlApiDoc

	buffer := []byte(docEntity.Content)
	err := yaml.Unmarshal(buffer, &doc)
	if err != nil {
		log.Warn(err)
		c.JSON(http.StatusBadRequest, Error{
			Code:    40007,
			Message: err.Error(),
		})
	}

	dbName := c.Query("dbname")
	log.WithFields(log.Fields{
		"dbname": dbName,
	}).Info("dbname")

	var dbConfig entity.DataBaseConfig
	err = s.Db.Get(&dbConfig, "SELECT * FROM database_config WHERE name=?", dbName)
	if err != nil {
		log.Error(err)
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

	sqlBuilder, err := sqlcomposer.NewSqlBuilder(db, []byte(docEntity.Content))
	if err != nil {
		log.Error(err)
		c.JSON(http.StatusBadRequest, Error{
			Code:    40010,
			Message: err.Error(),
		})
		return
	}
	err = configureSqlCompose(sqlBuilder)
	if err != nil {
		log.Fatal(err)
	}

	result := struct {
		Total int64         `json:"total,omitempty"`
		Data  []interface{} `json:"data,omitempty"`
	}{}
	for key := range doc.Composition.Subject {
		err = sqlBuilder.AddFilters(custFilters, sqlcomposer.AND)
		if err != nil {
			log.Error(err)
		}

		q, a, err := sqlBuilder.Limit((req.PageIndex-1)*req.PageLimit, req.PageLimit).Rebind(key)

		if err != nil {
			log.Error(err)
		}
		if key == "total" {
			var total int64
			err = db.QueryRowx(q, a...).Scan(&total)
			if err != nil {
				log.Error(err)
			}
			result.Total = total
		} else {
			rows, err := db.Queryx(q, a...)
			if err != nil {
				log.Error(err)
			}

			for rows.Next() {
				item := make(map[string]interface{})
				err := rows.MapScan(item)
				if err != nil {
					log.Error(err)
				}

				for k, encoded := range item {
					switch encoded.(type) {
					case []byte:
						item[k] = string(encoded.([]byte))
					}
				}

				result.Data = append(result.Data, item)
			}

			if err != nil {
				log.Error(err)
			}
		}

	}

	c.JSON(http.StatusOK, result)
}

type attrsTokenReplacer struct {
	Attrs map[string]string
	DB    *sqlx.DB
}

func (atr *attrsTokenReplacer) TokenReplace(ctx map[string]interface{}) string {
	return ProductAttrsToJoinInStat(atr.DB, atr.Attrs)
}

type attrsFieldsTokenReplacer struct {
	Attrs map[string]string
}

func (atr *attrsFieldsTokenReplacer) TokenReplace(ctx map[string]interface{}) string {
	return ProductAttrsToSelect(atr.Attrs)
}

var once sync.Once

type DictTypesSingleton map[string]string

var dictTypes DictTypesSingleton

type DictType struct {
	SID         string         `db:"sid"`
	Code        string         `db:"code"`
	Name        string         `db:"name"`
	Status      int            `db:"status"`
	Description string         `db:"description"`
	CreateTime  sql.NullString `db:"create_time"`
	UpdateTime  sql.NullString `db:"update_time"`
	IsDelete    int            `db:"is_delete"`
	ParentCode  sql.NullString `db:"parent_code"`
}

func GetMESDictTypes(db *sqlx.DB) DictTypesSingleton {
	once.Do(func() {
		dictTypes = DictTypesSingleton{}

		var types []DictType
		types = []DictType{}
		err := db.Select(&types, "SELECT * FROM fty_dictionary_type")

		if err != nil {
			log.Error(err)
		}

		for _, value := range types {
			dictTypes[value.Code] = value.SID
		}
	})

	return dictTypes
}

func ProductAttrsToJoinInStat(db *sqlx.DB, a map[string]string) string {
	var str []string

	dt := GetMESDictTypes(db)

	for key, _ := range a {
		alias := strings.Replace(key, "-", "_", -1)

		sid, ok := dt[key]

		if !ok {
			os.Exit(500)
		}

		str = append(str,
			fmt.Sprintf(`LEFT JOIN fty_obj_attr AS %s ON %s.attr_sid = '%s' AND %s.obj_sid = fty_product.sid`,
				alias, alias, sid, alias))
	}
	sort.Strings(str)
	return strings.Join(str, " ")
}

func ProductAttrsToSelect(a map[string]string) string {
	var str []string
	for key, value := range a {
		alias := strings.Replace(key, "-", "_", -1)
		str = append(str, fmt.Sprintf("%s.attr_value AS %s", alias, value))
	}
	sort.Strings(str)
	return strings.Join(str, ",")
}

func configureSqlCompose(sb *sqlcomposer.SqlBuilder) error {

	err := sb.RegisterToken("attrs", func(params []sqlcomposer.TokenParam) sqlcomposer.TokenReplacer {
		attrs := map[string]string{}
		for _, p := range params {
			attrs[p.Name] = p.Value
		}
		return &attrsTokenReplacer{
			Attrs: attrs,
			DB:    sb.DB,
		}
	})
	if err != nil {
		return err
	}
	err = sb.RegisterToken("attrs_fields", func(params []sqlcomposer.TokenParam) sqlcomposer.TokenReplacer {
		attrs := map[string]string{}
		for _, p := range params {
			attrs[p.Name] = p.Value
		}
		return &attrsFieldsTokenReplacer{
			Attrs: attrs,
		}
	})
	if err != nil {
		return err
	}
	return nil
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
