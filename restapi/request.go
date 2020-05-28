package restapi

type AddDbConfigRequest struct {
  Name string `json:"name"`
  Dsn  string `json:"dsn"`
}

type UpdateDbConfigRequest struct {
  Name string `json:"name"`
  Dsn  string `json:"dsn"`
}

type GetResultRequest struct {
  Filters []*GetResultFilterItem `json:"filters"`
}

type GetResultFilterItem struct {
  Key string `json:"key"`
  Op  string `json:"op"`
  Val string `json:"val"`
}
