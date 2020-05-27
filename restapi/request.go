package restapi

type AddDbConfigRequest struct {
  Name string `json:"name"`
  Dsn  string `json:"dsn"`
}

type UpdateDbConfigRequest struct {
  Name string `json:"name"`
  Dsn  string `json:"dsn"`
}
