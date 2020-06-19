package entity

//table
type Doc struct {
	ID        int     `db:"id" json:"-"`
	UUID      *string `db:"uuid" json:"uuid,omitempty"`
	Name      string  `json:"name"`
	Path      string  `json:"path,omitempty"`
	Content   *string  `json:"content,omitempty"`
	Desc      string  `db:"description" json:"description,omitempty"`
	DB        string  `db:"db_name" json:"db_name,omitempty"`
	CreatedAt *int    `db:"created_at" json:"created_at,omitempty"`
	UpdatedAt *int    `db:"updated_at" json:"updated_at,omitempty"`
	DeletedAt *int    `db:"deleted_at" json:"deleted_at,omitempty"`
}
