package entity

type DataBaseConfig struct {
  ID        int     `db:"id" json:"-"`
  UUID      *string `db:"uuid" json:"uuid,omitempty"`
  Name      string  `json:"name"`
  Dsn       string  `json:"-"`
  CreatedAt *int    `db:"created_at" json:"created_at,omitempty"`
  UpdatedAt *int    `db:"updated_at" json:"updated_at,omitempty"`
  DeletedAt *int    `db:"deleted_at" json:"deleted_at,omitempty"`
}
