package restapi

import "gitlab.com/beehplus/sql-compose/entity"

type DocListResult struct {
  Data  []*entity.Doc `json:"data"`
  Total int           `json:"total"`
}

type DbConfigList struct {
  Data  []*entity.DataBaseConfig `json:"data"`
  Total int                      `json:"total"`
}
