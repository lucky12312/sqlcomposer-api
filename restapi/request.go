package restapi

import "github.com/wangxb07/sqlcomposer"

type AddDbConfigRequest struct {
	Name string `json:"name"`
	Dsn  string `json:"dsn"`
}

type UpdateDbConfigRequest struct {
	Name string `json:"name"`
	Dsn  string `json:"dsn"`
}

type GetResultRequest struct {
	PageIndex int64                  `json:"page_index"`
	PageLimit int64                  `json:"page_limit"`
	Filters   []*GetResultFilterItem `json:"filters"`
}

type GetResultFilterItem struct {
	Attr string               `json:"attr"`
	Op   sqlcomposer.Operator `json:"op"`
	Val  interface{}       `json:"val"`
}
